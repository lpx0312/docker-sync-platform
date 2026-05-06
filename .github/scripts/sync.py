#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import os
import sys
import time
from typing import List, Dict, Tuple

IMAGES_FILE = "images.txt"
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "6"))
RETRY_COUNT = int(os.getenv("RETRY_COUNT", "2"))
PER_IMAGE_TIMEOUT = int(os.getenv("PER_IMAGE_TIMEOUT", str(20 * 60)))
LOG_FILE = os.getenv("SYNC_LOG_FILE", "sync.log")

ALIYUN_REGISTRY = os.getenv("ALIYUN_REGISTRY")
ALIYUN_NAME_SPACE = os.getenv("ALIYUN_NAME_SPACE")
ALIYUN_REGISTRY_USER = os.getenv("ALIYUN_REGISTRY_USER")
ALIYUN_REGISTRY_PASSWORD = os.getenv("ALIYUN_REGISTRY_PASSWORD")
DOCKERHUB_USERNAME = os.getenv("DOCKERHUB_USERNAME")
DOCKERHUB_PASSWORD = os.getenv("DOCKERHUB_PASSWORD")

ARCH_CACHE: Dict[str, List[str]] = {}

if not all([ALIYUN_REGISTRY, ALIYUN_NAME_SPACE, ALIYUN_REGISTRY_USER, ALIYUN_REGISTRY_PASSWORD]):
    print("ERROR: missing aliyun registry env", file=sys.stderr)
    sys.exit(1)

# ---------------------------
# log
# ---------------------------
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

# ---------------------------
# run command
# ---------------------------
async def run_cmd(cmd: List[str], timeout: int = None) -> Tuple[int, str, str]:
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        outs, errs = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return proc.returncode, outs.decode(errors="ignore"), errs.decode(errors="ignore")
    except asyncio.TimeoutError:
        try:
            proc.kill()
        except:
            pass
        return 124, "", f"TIMEOUT after {timeout}s"
    except Exception as e:
        return 125, "", str(e)

# ---------------------------
# normalize image
# ---------------------------
def normalize_image_reference(image: str):
    image = image.strip()

    if "/" in image:
        first_part = image.split("/")[0]

        if "." in first_part or first_part == "localhost":
            source_ref = f"docker://{image}"
            clean_name = image.split("/", 1)[1]
            return source_ref, clean_name

        if ":" in first_part:
            source_ref = f"docker://{image}"
            clean_name = image.split("/", 1)[1]
            return source_ref, clean_name

    if image.count("/") == 0:
        source_ref = f"docker://docker.io/library/{image}"
        clean_name = image
    else:
        source_ref = f"docker://docker.io/{image}"
        clean_name = image

    return source_ref, clean_name

# ---------------------------
# login
# ---------------------------
async def skopeo_login():
    _log("[LOGIN] aliyun registry")
    rc, out, err = await run_cmd([
        "skopeo", "login",
        "-u", ALIYUN_REGISTRY_USER,
        "-p", ALIYUN_REGISTRY_PASSWORD,
        ALIYUN_REGISTRY
    ], timeout=60)
    if rc != 0:
        _log(err)
        sys.exit(1)

    _log("[LOGIN] dockerhub")
    rc, out, err = await run_cmd([
        "skopeo", "login",
        "-u", DOCKERHUB_USERNAME,
        "-p", DOCKERHUB_PASSWORD,
        "docker.io"
    ], timeout=60)
    if rc != 0:
        _log(err)
        sys.exit(1)

# ---------------------------
# parse images
# ---------------------------
def parse_images_file(path: str) -> List[str]:
    if not os.path.exists(path):
        _log(f"images file not found: {path}")
        sys.exit(1)

    lines = []
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            lines.append(line)
    return lines

# ---------------------------
# duplicate detect
# ---------------------------
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

        if image_name in temp_map and temp_map[image_name] != namespace:
            duplicates[image_name] = True
        else:
            temp_map[image_name] = namespace

    return duplicates

# ---------------------------
# detect architectures
# ---------------------------
async def detect_source_architectures(image: str) -> List[str]:
    source, _ = normalize_image_reference(image)

    rc, out, err = await run_cmd([
        "skopeo", "inspect", "--raw", source
    ], timeout=120)

    if rc != 0:
        _log(f"[ARCH DETECT] inspect failed for {image}: {err}")
        return []

    archs = set()

    try:
        data = json.loads(out)

        if "manifests" in data:
            for item in data["manifests"]:
                platform = item.get("platform", {})
                arch = platform.get("architecture")
                os_name = platform.get("os")
                if os_name == "linux" and arch:
                    archs.add(arch)

        elif "architecture" in data:
            arch = data.get("architecture")
            os_name = data.get("os", "linux")
            if os_name == "linux" and arch:
                archs.add(arch)

    except Exception as e:
        _log(f"[ARCH DETECT] parse failed for {image}: {e}")
        return []

    return list(archs)

async def prefetch_all_architectures(images: List[str]):
    global ARCH_CACHE
    _log("=== PREFETCH IMAGE ARCHITECTURES START ===")
    sem = asyncio.Semaphore(8)

    async def _worker(img: str):
        async with sem:
            archs = await detect_source_architectures(img)
            ARCH_CACHE[img] = archs
            _log(f"[PREFETCH] {img} -> {archs}")

    await asyncio.gather(*[_worker(i) for i in images])
    _log("=== PREFETCH IMAGE ARCHITECTURES END ===")

# ---------------------------
# build copy cmd
# ---------------------------
def build_arch_copy_cmd(image: str, arch: str, duplicates: Dict[str, bool]):
    source, clean_name = normalize_image_reference(image)

    image_no_digest = clean_name.split("@")[0]
    parts = image_no_digest.split("/")
    image_name_tag = parts[-1]
    image_name = image_name_tag.split(":")[0]

    prefix = ""
    if image_name in duplicates:
        if len(parts) >= 2:
            prefix = parts[-2] + "_"

    tmp_target = f"{ALIYUN_REGISTRY}/{ALIYUN_NAME_SPACE}/{prefix}{image_name_tag}-{arch}-tmp"
    final_target = f"{ALIYUN_REGISTRY}/{ALIYUN_NAME_SPACE}/{prefix}{image_name_tag}"

    cmd = [
        "skopeo", "copy",
        "--override-os", "linux",
        "--override-arch", arch,
        source,
        f"docker://{tmp_target}"
    ]

    return source, tmp_target, final_target, cmd

# ---------------------------
# delete image
# ---------------------------
async def delete_image(target: str):
    await run_cmd([
        "skopeo", "delete",
        "--creds", f"{ALIYUN_REGISTRY_USER}:{ALIYUN_REGISTRY_PASSWORD}",
        f"docker://{target}"
    ], timeout=60)

# ---------------------------
# merge manifest
# ---------------------------
async def merge_manifest(final_target: str, template_target: str, index: int):
    cmd = [
        "manifest-tool",
        "--username", ALIYUN_REGISTRY_USER,
        "--password", ALIYUN_REGISTRY_PASSWORD,
        "push", "from-args",
        "--platforms", "linux/amd64,linux/arm64",
        "--template", template_target,
        "--target", final_target
    ]

    _log(f"[{index}] MERGE -> {final_target}")
    rc, out, err = await run_cmd(cmd, timeout=300)
    if rc != 0:
        _log(f"[{index}] MERGE FAILED: {err}")
        return False
    return True

# ---------------------------
# parallel copy
# ---------------------------
async def run_copy_cmd(cmd: List[str], arch: str, index: int):
    _log(f"[{index}] COPY {arch} START")
    rc, out, err = await run_cmd(cmd, timeout=PER_IMAGE_TIMEOUT)
    if rc != 0:
        raise Exception(f"{arch} copy failed: {err}")
    _log(f"[{index}] COPY {arch} DONE")

# ---------------------------
# sync task
# ---------------------------
async def sync_image_task(image: str, duplicates: Dict[str, bool], semaphore: asyncio.Semaphore, index: int):
    async with semaphore:
        start_ts = time.time()
        final_target = ""

        for attempt in range(1, RETRY_COUNT + 2):
            amd_tmp = None
            arm_tmp = None

            try:
                _log(f"[{index}] START {image} attempt={attempt}")
                supported_archs = ARCH_CACHE.get(image, [])
                _log(f"[{index}] SUPPORTED ARCHS: {supported_archs}")

                if not supported_archs:
                    raise Exception("no supported architecture")

                copy_tasks = []

                if "amd64" in supported_archs:
                    _, amd_tmp, final_target, cmd_amd = build_arch_copy_cmd(image, "amd64", duplicates)
                    await delete_image(amd_tmp)
                    copy_tasks.append(run_copy_cmd(cmd_amd, "amd64", index))

                if "arm64" in supported_archs:
                    _, arm_tmp, final_target, cmd_arm = build_arch_copy_cmd(image, "arm64", duplicates)
                    await delete_image(arm_tmp)
                    copy_tasks.append(run_copy_cmd(cmd_arm, "arm64", index))

                await asyncio.gather(*copy_tasks)

                await delete_image(final_target)

                if "amd64" in supported_archs and "arm64" in supported_archs:
                    image_name_tag = final_target.split("/")[-1]
                    template = final_target.replace(image_name_tag, image_name_tag + "-ARCH-tmp")

                    merged = await merge_manifest(final_target, template, index)
                    if not merged:
                        raise Exception("manifest merge failed")

                    if amd_tmp:
                        await delete_image(amd_tmp)
                    if arm_tmp:
                        await delete_image(arm_tmp)

                else:
                    single_arch = "amd64" if "amd64" in supported_archs else "arm64"
                    source_ref, _ = normalize_image_reference(image)

                    cmd_direct = [
                        "skopeo", "copy",
                        "--override-os", "linux",
                        "--override-arch", single_arch,
                        source_ref,
                        f"docker://{final_target}"
                    ]

                    rc, out, err = await run_cmd(cmd_direct, timeout=PER_IMAGE_TIMEOUT)
                    if rc != 0:
                        raise Exception(f"single arch final push failed: {err}")

                    if amd_tmp:
                        await delete_image(amd_tmp)
                    if arm_tmp:
                        await delete_image(arm_tmp)

                elapsed = time.time() - start_ts
                _log(f"[{index}] SUCCESS ({elapsed:.1f}s) -> {final_target}")
                return 0, final_target

            except Exception as e:
                _log(f"[{index}] FAILED attempt={attempt}: {e}")
                if attempt <= RETRY_COUNT:
                    backoff = 2 ** (attempt - 1)
                    _log(f"[{index}] retry after {backoff}s")
                    await asyncio.sleep(backoff)
                else:
                    if amd_tmp:
                        await delete_image(amd_tmp)
                    if arm_tmp:
                        await delete_image(arm_tmp)
                    return 1, final_target or image

# ---------------------------
# main
# ---------------------------
async def main():
    _open_log()
    _log(f"CONFIG: MAX_CONCURRENT={MAX_CONCURRENT} RETRY_COUNT={RETRY_COUNT} PER_IMAGE_TIMEOUT={PER_IMAGE_TIMEOUT}")

    await skopeo_login()

    lines = parse_images_file(IMAGES_FILE)
    duplicates = detect_duplicates(lines)

    await prefetch_all_architectures(lines)

    _log(f"TOTAL IMAGES: {len(lines)}")
    _log(f"DUPLICATES: {list(duplicates.keys())}")

    sem = asyncio.Semaphore(MAX_CONCURRENT)
    tasks = [sync_image_task(img, duplicates, sem, i) for i, img in enumerate(lines, 1)]
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

    for item in failed:
        _log(f"FAILED IMAGE: {item}")

    _close_log()

    if failed:
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
