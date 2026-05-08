#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import os
import sys
import time
from typing import List, Dict

IMAGES_FILE = "images.txt"

# 并发不要太高，DockerHub 很容易限流
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "4"))

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
async def run_cmd(cmd, timeout=None):

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

            clean_name = image.split("/", 1)[1]

            return image, clean_name

    # dockerhub library
    if image.count("/") == 0:

        source_ref = f"docker.io/library/{image}"

        clean_name = image

    else:

        source_ref = f"docker.io/{image}"

        clean_name = image

    return source_ref, clean_name


# --------------------------------------------------
# login
# --------------------------------------------------
async def crane_login():

    _log("[LOGIN] aliyun registry")

    rc, out, err = await run_cmd([
        "crane",
        "auth",
        "login",
        ALIYUN_REGISTRY,
        "-u",
        ALIYUN_REGISTRY_USER,
        "-p",
        ALIYUN_REGISTRY_PASSWORD
    ], timeout=60)

    if rc != 0:
        _log(err)
        sys.exit(1)

    if DOCKERHUB_USERNAME and DOCKERHUB_PASSWORD:

        _log("[LOGIN] dockerhub")

        rc, out, err = await run_cmd([
            "crane",
            "auth",
            "login",
            "docker.io",
            "-u",
            DOCKERHUB_USERNAME,
            "-p",
            DOCKERHUB_PASSWORD
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
def parse_images_file(path: str):

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
def detect_duplicates(lines: List[str]):

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
# artifact detect
# --------------------------------------------------
async def is_real_image_manifest(source_ref: str, digest: str):

    rc, out, err = await run_cmd([
        "crane",
        "manifest",
        f"{source_ref}@{digest}"
    ], timeout=120)

    if rc != 0:
        return False

    try:

        manifest_json = json.loads(out)

        config = manifest_json.get("config", {})

        config_media_type = config.get("mediaType", "")

        # OCI artifact
        if config_media_type == "application/vnd.oci.empty.v1+json":
            return False

        return True

    except:
        return False


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

                # --------------------------------------------------
                # 获取 manifest list
                # --------------------------------------------------
                rc, out, err = await run_cmd([
                    "crane",
                    "manifest",
                    source_ref
                ], timeout=120)

                if rc != 0:
                    raise Exception(err)

                manifest_json = json.loads(out)

                manifests = manifest_json.get("manifests", [])

                if not manifests:
                    raise Exception("no manifests found")

                copied_refs = []

                # --------------------------------------------------
                # copy 每个架构
                # --------------------------------------------------
                for item in manifests:

                    digest = item.get("digest")

                    platform = item.get("platform", {})

                    arch = platform.get("architecture")

                    os_name = platform.get("os")

                    variant = platform.get("variant")

                    # artifact 通常没 platform
                    if not arch or not os_name:

                        _log(
                            f"[{index}] SKIP artifact(no platform) "
                            f"{digest}"
                        )

                        continue

                    # 二次检查
                    ok = await is_real_image_manifest(
                        source_ref,
                        digest
                    )

                    if not ok:

                        _log(
                            f"[{index}] SKIP artifact(empty config) "
                            f"{digest}"
                        )

                        continue

                    src_digest_ref = f"{source_ref}@{digest}"

                    _log(
                        f"[{index}] COPY "
                        f"{os_name}/{arch} "
                        f"{digest}"
                    )

                    rc2, out2, err2 = await run_cmd([
                        "crane",
                        "copy",
                        src_digest_ref,
                        final_target
                    ], timeout=PER_IMAGE_TIMEOUT)

                    if rc2 != 0:
                        raise Exception(err2)

                    copied_refs.append({
                        "digest": digest,
                        "arch": arch,
                        "os": os_name,
                        "variant": variant
                    })

                if not copied_refs:
                    raise Exception("no valid image manifests")

                # --------------------------------------------------
                # 创建 manifest list
                # --------------------------------------------------
                _log(f"[{index}] CREATE manifest list")

                await run_cmd([
                    "docker",
                    "manifest",
                    "rm",
                    final_target
                ], timeout=30)

                create_cmd = [
                    "docker",
                    "manifest",
                    "create",
                    final_target
                ]

                for item in copied_refs:

                    create_cmd.append(
                        f"{final_target}@{item['digest']}"
                    )

                rc3, out3, err3 = await run_cmd(
                    create_cmd,
                    timeout=120
                )

                if rc3 != 0:
                    raise Exception(err3)

                # annotate
                for item in copied_refs:

                    annotate_cmd = [
                        "docker",
                        "manifest",
                        "annotate",
                        final_target,
                        f"{final_target}@{item['digest']}",
                        "--arch",
                        item["arch"],
                        "--os",
                        item["os"]
                    ]

                    if item["variant"]:

                        annotate_cmd.extend([
                            "--variant",
                            item["variant"]
                        ])

                    await run_cmd(
                        annotate_cmd,
                        timeout=60
                    )

                # push
                rc4, out4, err4 = await run_cmd([
                    "docker",
                    "manifest",
                    "push",
                    "--purge",
                    final_target
                ], timeout=PER_IMAGE_TIMEOUT)

                if rc4 != 0:
                    raise Exception(err4)

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

                if "toomanyrequests" in err_msg.lower():
                    backoff = 300
                else:
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

    await crane_login()

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
