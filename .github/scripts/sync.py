#!/usr/bin/env python3
# .github/scripts/sync.py
# Python 3.12 compatible
"""
并行 Skopeo 同步脚本（适用于 Skopeo 1.13.1）
- 并发执行 skopeo copy（通过 asyncio.subprocess）
- 不使用 --image-parallel-copies
- 支持重试、超时、详细日志、summary
"""

import asyncio
import os
import re
import sys
import time
from typing import List, Dict, Tuple

# ---------------------------
# 配置（按需调整）
# ---------------------------
IMAGES_FILE = "images.txt"
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "8"))  # 并发任务数
RETRY_COUNT = int(os.getenv("RETRY_COUNT", "2"))       # 失败重试次数（不含首次尝试）
PER_IMAGE_TIMEOUT = int(os.getenv("PER_IMAGE_TIMEOUT", str(20 * 60)))  # 单镜像超时秒，默认 20min
LOG_FILE = os.getenv("SYNC_LOG_FILE", "sync.log")

# GitHub Action 环境变量（从 workflow env/secrets 传入）
ALIYUN_REGISTRY = os.getenv("ALIYUN_REGISTRY")
ALIYUN_NAME_SPACE = os.getenv("ALIYUN_NAME_SPACE")
ALIYUN_REGISTRY_USER = os.getenv("ALIYUN_REGISTRY_USER")
ALIYUN_REGISTRY_PASSWORD = os.getenv("ALIYUN_REGISTRY_PASSWORD")

if not all([ALIYUN_REGISTRY, ALIYUN_NAME_SPACE, ALIYUN_REGISTRY_USER, ALIYUN_REGISTRY_PASSWORD]):
    print("ERROR: 必须设置 ALIYUN_REGISTRY/ALIYUN_NAME_SPACE/ALIYUN_REGISTRY_USER/ALIYUN_REGISTRY_PASSWORD 环境变量", file=sys.stderr)
    sys.exit(1)

# ---------------------------
# 日志工具（同时打印并写文件）
# ---------------------------
_log_fh = None
def _open_log():
    global _log_fh
    _log_fh = open(LOG_FILE, "a", encoding="utf-8")
    _log("=== START SYNC LOG ===\n")

def _close_log():
    global _log_fh
    if _log_fh:
        _log("=== END SYNC LOG ===\n")
        _log_fh.close()
        _log_fh = None

def _log(msg: str):
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    line = f"[{ts}] {msg}"
    print(line)
    if _log_fh:
        _log_fh.write(line + "\n")
        _log_fh.flush()

# ---------------------------
# asyncio 命令执行
# ---------------------------
async def run_cmd(cmd: List[str], timeout: int = None) -> Tuple[int, str, str]:
    """
    返回 (returncode, stdout, stderr)
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        outs, errs = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        return proc.returncode, (outs.decode(errors="ignore") if outs else ""), (errs.decode(errors="ignore") if errs else "")
    except asyncio.TimeoutError:
        # 超时：杀掉进程
        try:
            proc.kill()
        except Exception:
            pass
        return 124, "", f"TIMEOUT after {timeout}s"
    except Exception as e:
        return 125, "", f"EXCEPTION: {e}"

# ---------------------------
# 登录函数（skopeo login）
# ---------------------------
async def skopeo_login():
    _log(f"[LOGIN] Attempting skopeo login to {ALIYUN_REGISTRY}")
    cmd = ["skopeo", "login", "-u", ALIYUN_REGISTRY_USER, "-p", ALIYUN_REGISTRY_PASSWORD, ALIYUN_REGISTRY]
    rc, out, err = await run_cmd(cmd, timeout=60)
    if rc != 0:
        _log(f"[LOGIN] FAILED rc={rc}\nSTDOUT: {out}\nSTDERR: {err}")
        raise SystemExit(1)
    _log(f"[LOGIN] Success\n{out}")

# ---------------------------
# 解析 images.txt
# ---------------------------
def parse_images_file(path: str) -> List[str]:
    lines: List[str] = []
    if not os.path.exists(path):
        _log(f"ERROR: images file not found: {path}")
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            lines.append(line)
    return lines

# ---------------------------
# 检测重复镜像名字（保留原有逻辑）
# ---------------------------
def detect_duplicates(lines: List[str]) -> Dict[str, bool]:
    temp_map: Dict[str, str] = {}
    duplicates: Dict[str, bool] = {}
    for line in lines:
        image = line.split()[-1]
        image = image.split("@")[0]  # 去掉 digest
        parts = image.split("/")
        image_name_tag = parts[-1]
        image_name = image_name_tag.split(":")[0]

        if len(parts) == 3:
            namespace = parts[1]
        elif len(parts) == 2:
            namespace = parts[0]
        else:
            namespace = ""
        namespace = f"{namespace}_"

        if image_name in temp_map and temp_map[image_name] != namespace:
            duplicates[image_name] = True
        else:
            temp_map[image_name] = namespace
    return duplicates

# ---------------------------
# 构造 skopeo copy 命令（不包含 --image-parallel-copies）
# ---------------------------
def build_skopeo_copy_cmd(line: str, duplicates: Dict[str,bool]) -> Tuple[str, str, List[str]]:
    # 解析 platform 参数：支持 "--platform linux/arm64" 或 "--platform=linux/arm64"
    platform = None
    m = re.search(r"--platform(?:[ =])([^ \t]+)", line)
    if m:
        platform = m.group(1).strip()

    image = line.split()[-1]
    image_no_digest = image.split("@")[0]
    parts = image_no_digest.split("/")
    image_name_tag = parts[-1]
    image_name = image_name_tag.split(":")[0]

    prefix = ""
    if image_name in duplicates:
        if len(parts) >= 2:
            prefix = parts[-2] + "_"
        else:
            prefix = ""

    platform_suffix = ""
    if platform:
        platform_suffix = "-" + platform.replace("/", "-")

    source = f"docker://{image_no_digest}"
    target = f"docker://{ALIYUN_REGISTRY}/{ALIYUN_NAME_SPACE}/{prefix}{image_name_tag}{platform_suffix}"

    cmd = ["skopeo", "copy"]
    # 如果指定 platform，使用 override-os/override-arch（skopeo 支持）
    if platform:
        # 如果用户写错 platform（比如只有 arch），尽量鲁棒处理
        parts_p = platform.split("/")
        override_os = parts_p[0] if len(parts_p) >= 1 and parts_p[0] else "linux"
        override_arch = parts_p[1] if len(parts_p) >= 2 and parts_p[1] else "amd64"
        cmd += ["--override-os", override_os, "--override-arch", override_arch]

    cmd += [source, target]
    return source, target, cmd

# ---------------------------
# 单条同步任务（包含重试）
# ---------------------------
async def sync_image_task(line: str, duplicates: Dict[str,bool], semaphore: asyncio.Semaphore, index: int) -> Tuple[int, str]:
    """
    返回 (rc, target)
    """
    async with semaphore:
        source, target, cmd = build_skopeo_copy_cmd(line, duplicates)
        attempt = 0
        last_err = ""
        start_ts = time.time()
        while attempt <= RETRY_COUNT:
            attempt += 1
            _log(f"[{index}] START attempt={attempt}  Source={source}  Target={target}")
            _log(f"[{index}] CMD: {' '.join(cmd)}")
            rc, out, err = await run_cmd(cmd, timeout=PER_IMAGE_TIMEOUT)
            elapsed = time.time() - start_ts
            if rc == 0:
                _log(f"[{index}] SUCCESS (attempt={attempt}) ({elapsed:.1f}s) -> {target}")
                if out:
                    _log(f"[{index}] STDOUT:\n{out.strip()}")
                return 0, target
            else:
                last_err = err or out or f"rc={rc}"
                _log(f"[{index}] FAILED (attempt={attempt}) rc={rc} ({elapsed:.1f}s)")
                if out:
                    _log(f"[{index}] STDOUT:\n{out.strip()}")
                if err:
                    _log(f"[{index}] STDERR:\n{err.strip()}")
                if attempt <= RETRY_COUNT:
                    backoff = 2 ** (attempt - 1)
                    _log(f"[{index}] Retrying after {backoff}s...")
                    await asyncio.sleep(backoff)
                else:
                    _log(f"[{index}] Exhausted retries for {target}")
                    return rc or 1, target

# ---------------------------
# 主入口
# ---------------------------
async def main():
    _open_log()
    _log(f"CONFIG: MAX_CONCURRENT={MAX_CONCURRENT} RETRY_COUNT={RETRY_COUNT} PER_IMAGE_TIMEOUT={PER_IMAGE_TIMEOUT}s")

    # 检查 skopeo 是否存在
    rc, out, err = await run_cmd(["skopeo", "--version"], timeout=10)
    if rc != 0:
        _log(f"ERROR: skopeo not found or failed to run. rc={rc}\n{err}")
        _close_log()
        sys.exit(1)
    _log(f"Skopeo version: {out.strip()}")

    # login
    await skopeo_login()

    # parse images
    lines = parse_images_file(IMAGES_FILE)
    duplicates = detect_duplicates(lines)
    _log(f"Parsed {len(lines)} images. Duplicates: {list(duplicates.keys())}")

    sem = asyncio.Semaphore(MAX_CONCURRENT)
    tasks = []
    for i, line in enumerate(lines, start=1):
        tasks.append(sync_image_task(line, duplicates, sem, i))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    total = len(lines)
    success = 0
    failed_items = []
    for r in results:
        if isinstance(r, tuple):
            rc, target = r
            if rc == 0:
                success += 1
            else:
                failed_items.append((target, rc))
        else:
            # 异常
            _log(f"Task exception: {r}")
            failed_items.append(("unknown", 1))

    _log("\n===== SUMMARY =====")
    _log(f"Total: {total}, Success: {success}, Failed: {len(failed_items)}")
    if failed_items:
        for t, rc in failed_items:
            _log(f"FAILED: {t} rc={rc}")

    _close_log()
    if failed_items:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        _log("Interrupted by user")
        _close_log()
        raise
