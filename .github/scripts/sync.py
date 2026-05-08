#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import sys
import time
from typing import List, Dict, Tuple

IMAGES_FILE = "images.txt"

# 并发不要太高，DockerHub 很容易限流
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "6"))

# 重试次数
RETRY_COUNT = int(os.getenv("RETRY_COUNT", "2"))

# 单镜像超时
PER_IMAGE_TIMEOUT = int(os.getenv("PER_IMAGE_TIMEOUT", str(20 * 60)))

LOG_FILE = os.getenv("SYNC_LOG_FILE", "sync.log")

ALIYUN_REGISTRY = os.getenv("ALIYUN_REGISTRY")
ALIYUN_NAME_SPACE = os.getenv("ALIYUN_NAME_SPACE")
ALIYUN_REGISTRY_USER = os.getenv("ALIYUN_REGISTRY_USER")
ALIYUN_REGISTRY_PASSWORD = os.getenv("ALIYUN_REGISTRY_PASSWORD")

DOCKERHUB_USERNAME = os.getenv("DOCKERHUB_USERNAME")
DOCKERHUB_PASSWORD = os.getenv("DOCKERHUB_PASSWORD")

if not all([
    ALIYUN_REGISTRY,
    ALIYUN_NAME_SPACE,
    ALIYUN_REGISTRY_USER,
    ALIYUN_REGISTRY_PASSWORD
]):
    print("ERROR: missing aliyun registry env", file=sys.stderr)
    sys.exit(1)

# --------------------------------------------------
# log
# --------------------------------------------------
_log_fh = None


def _open_log():
    global _log_fh
    _log_fh = open(LOG_FILE, "a", encoding="utf-8")
    _log("=== START SYNC LOG ===")


def _close_log():
    global _log_fh
    if _log_fh:
        _log("=== END SYNC LOG ===")
        _log_fh.close()
        _log_fh = None


def _log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    line = f"[{ts}] {msg}"
    print(line, flush=True)

    if _log_fh:
        _log_fh.write(line + "\n")
        _log_fh.flush()


# --------------------------------------------------
# run command
# --------------------------------------------------
async def run_cmd(cmd: List[str], timeout: int = None) -> Tuple[int, str, str]:
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        outs, errs = await asyncio.wait_for(
            proc.communicate(),
            timeout=timeout
        )

        return (
            proc.returncode,
            outs.decode(errors="ignore"),
            errs.decode(errors="ignore")
        )

    except asyncio.TimeoutError:
        try:
            proc.kill()
        except:
            pass

        return 124, "", f"TIMEOUT after {timeout}s"

    except Exception as e:
        return 125, "", str(e)


# --------------------------------------------------
# normalize image
# --------------------------------------------------
def normalize_image_reference(image: str):
    image = image.strip()

    if "/" in image:
        first_part = image.split("/")[0]

        # 已带 registry
        if "." in first_part or ":" in first_part or first_part == "localhost":
            source_ref = f"docker://{image}"
            clean_name = image.split("/", 1)[1]
            return source_ref, clean_name

    # dockerhub library
    if image.count("/") == 0:
        source_ref = f"docker://docker.io/library/{image}"
        clean_name = image
    else:
        source_ref = f"docker://docker.io/{image}"
        clean_name = image

    return source_ref, clean_name


# --------------------------------------------------
# login
# --------------------------------------------------
async def skopeo_login():
    _log("[LOGIN] aliyun registry")

    rc, out, err = await run_cmd([
        "skopeo",
        "login",
        "-u",
        ALIYUN_REGISTRY_USER,
        "-p",
        ALIYUN_REGISTRY_PASSWORD,
        ALIYUN_REGISTRY
    ], timeout=60)

    if rc != 0:
        _log(err)
        sys.exit(1)

    if DOCKERHUB_USERNAME and DOCKERHUB_PASSWORD:
        _log("[LOGIN] dockerhub")

        rc, out, err = await run_cmd([
            "skopeo",
            "login",
            "-u",
            DOCKERHUB_USERNAME,
            "-p",
            DOCKERHUB_PASSWORD,
            "docker.io"
        ], timeout=60)

        if rc != 0:
            _log(f"[WARN] dockerhub login failed: {err}")
        else:
            _log("[LOGIN] dockerhub success")
    else:
        _log("[WARN] dockerhub credential not found, skip login")


# --------------------------------------------------
# parse images
# --------------------------------------------------
def parse_images_file(path: str) -> List[str]:
    if not os.path.exists(path):
        _log(f"images file not found: {path}")
        sys.exit(1)

    lines = []

    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()

            if not line:
                continue

            if line.startswith("#"):
                continue

            lines.append(line)

    return lines


# --------------------------------------------------
# duplicate detect
# --------------------------------------------------
def detect_duplicates(lines: List[str]) -> Dict[str, bool]:
    temp_map = {}
    duplicates = {}

    for image in lines:
        _, clean_name = normalize_image_reference(image)

        image_no_digest = clean_name.split("@")[0]

        parts = image_no_digest.split("/")

        image_name_tag = parts[-1]
        image_name = image_name_tag.split(":")[0]

        namespace = parts[-2] if len(parts) >= 2 else "library"

        if image_name in temp_map:
            if temp_map[image_name] != namespace:
                duplicates[image_name] = True
        else:
            temp_map[image_name] = namespace

    return duplicates


# --------------------------------------------------
# build target
# --------------------------------------------------
def build_target(image: str, duplicates: Dict[str, bool]):
    _, clean_name = normalize_image_reference(image)

    image_no_digest = clean_name.split("@")[0]

    parts = image_no_digest.split("/")

    image_name_tag = parts[-1]
    image_name = image_name_tag.split(":")[0]

    prefix = ""

    # 不同 namespace 同名镜像
    if image_name in duplicates:
        if len(parts) >= 2:
            prefix = parts[-2] + "_"

    return f"{ALIYUN_REGISTRY}/{ALIYUN_NAME_SPACE}/{prefix}{image_name_tag}"


# --------------------------------------------------
# sync task
# --------------------------------------------------
async def sync_image_task(
    image: str,
    duplicates: Dict[str, bool],
    semaphore: asyncio.Semaphore,
    index: int
):
    async with semaphore:

        start_ts = time.time()

        source_ref, _ = normalize_image_reference(image)

        final_target = build_target(image, duplicates)

        for attempt in range(1, RETRY_COUNT + 2):

            try:
                _log(f"[{index}] START {image} attempt={attempt}")

                cmd = [
                    "skopeo",
                    "copy",
                
                    # multiarch
                    "--all",
                
                    # 关键：兼容 ACR 个人版
                    "--format", "v2s2",
                
                    # 去掉 OCI signature / artifact
                    "--remove-signatures",
                
                    # retry
                    "--retry-times", "3",
                
                    source_ref,
                    f"docker://{final_target}"
                ]

                _log(f"[{index}] CMD: {' '.join(cmd)}")

                rc, out, err = await run_cmd(
                    cmd,
                    timeout=PER_IMAGE_TIMEOUT
                )

                if rc != 0:
                    raise Exception(err)

                elapsed = time.time() - start_ts

                _log(
                    f"[{index}] SUCCESS "
                    f"({elapsed:.1f}s) -> {final_target}"
                )

                return 0, final_target

            except Exception as e:

                err_msg = str(e)

                _log(
                    f"[{index}] FAILED "
                    f"attempt={attempt}: {err_msg}"
                )

                # DockerHub rate limit 特殊退避
                if "toomanyrequests" in err_msg.lower():
                    backoff = 300
                else:
                    # 指数退避
                    backoff = min(30 * (2 ** (attempt - 1)), 300)

                if attempt <= RETRY_COUNT:
                    _log(f"[{index}] retry after {backoff}s")
                    await asyncio.sleep(backoff)
                else:
                    return 1, final_target


# --------------------------------------------------
# main
# --------------------------------------------------
async def main():
    _open_log()

    _log(
        f"CONFIG: "
        f"MAX_CONCURRENT={MAX_CONCURRENT} "
        f"RETRY_COUNT={RETRY_COUNT} "
        f"PER_IMAGE_TIMEOUT={PER_IMAGE_TIMEOUT}"
    )

    await skopeo_login()

    lines = parse_images_file(IMAGES_FILE)

    duplicates = detect_duplicates(lines)

    _log(f"TOTAL IMAGES: {len(lines)}")

    if duplicates:
        _log(f"DUPLICATES: {list(duplicates.keys())}")

    sem = asyncio.Semaphore(MAX_CONCURRENT)

    tasks = [
        sync_image_task(img, duplicates, sem, i)
        for i, img in enumerate(lines, 1)
    ]

    results = await asyncio.gather(*tasks)

    success = 0
    failed = []

    for rc, target in results:
        if rc == 0:
            success += 1
        else:
            failed.append(target)

    _log("===== SUMMARY =====")

    _log(f"SUCCESS: {success}")
    _log(f"FAILED : {len(failed)}")

    if failed:
        for item in failed:
            _log(f"FAILED IMAGE: {item}")

    _close_log()

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
