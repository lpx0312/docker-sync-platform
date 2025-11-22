import asyncio
import subprocess
import os
import re
from typing import Dict, List

# =========================================================
# 读取环境变量（GitHub Action 注入）
# =========================================================
ALIYUN_REGISTRY = os.getenv("ALIYUN_REGISTRY")
ALIYUN_NAME_SPACE = os.getenv("ALIYUN_NAME_SPACE")
ALIYUN_REGISTRY_USER = os.getenv("ALIYUN_REGISTRY_USER")
ALIYUN_REGISTRY_PASSWORD = os.getenv("ALIYUN_REGISTRY_PASSWORD")

IMAGES_FILE = "images.txt"
MAX_CONCURRENT = 8   # 可调整并发量


# =========================================================
# 工具函数：执行命令并捕获输出
# =========================================================
async def run_cmd(cmd: List[str]) -> (int, str, str):
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await process.communicate()
    return process.returncode, out.decode(), err.decode()


# =========================================================
# 登录阿里云镜像仓库
# =========================================================
async def docker_login():
    print(f"[LOGIN] Logging in {ALIYUN_REGISTRY} ...")

    rc, out, err = await run_cmd([
        "skopeo", "login",
        "-u", ALIYUN_REGISTRY_USER,
        "-p", ALIYUN_REGISTRY_PASSWORD,
        ALIYUN_REGISTRY
    ])

    if rc != 0:
        print("❌ Login failed")
        print(err)
        raise SystemExit(1)

    print("✅ Login success\n")


# =========================================================
# 解析 images.txt，原逻辑完全重写为 Python
# =========================================================
def parse_images() -> List[str]:
    lines = []
    with open(IMAGES_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            lines.append(line)
    return lines


# =========================================================
# 第一遍扫描：检测重复镜像名（保持你原逻辑）
# =========================================================
def detect_duplicate_images(lines: List[str]) -> Dict[str, bool]:
    temp_map = {}
    duplicates = {}

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


# =========================================================
# 构造 skopeo copy 命令（带平台处理）
# =========================================================
def build_skopeo_cmd(line: str, duplicates: Dict[str, bool]) -> (str, str, List[str]):
    platform = None

    # 解析平台
    m = re.search(r"--platform[ =]([^ ]+)", line)
    if m:
        platform = m.group(1)

    # 镜像本体
    image = line.split()[-1]
    image = image.split("@")[0]

    parts = image.split("/")
    image_name_tag = parts[-1]
    image_name = image_name_tag.split(":")[0]

    # 命名空间前缀逻辑（同 Bash）
    if image_name in duplicates:
        if len(parts) >= 2:
            prefix = parts[-2] + "_"
        else:
            prefix = ""
    else:
        prefix = ""

    # 平台后缀
    platform_suffix = ""
    if platform:
        platform_suffix = "-" + platform.replace("/", "-")

    SOURCE = f"docker://{image}"
    TARGET = f"docker://{ALIYUN_REGISTRY}/{ALIYUN_NAME_SPACE}/{prefix}{image_name_tag}{platform_suffix}"

    # 构造 skopeo 指令
    cmd = ["skopeo", "copy"]

    if platform:
        os_, arch = platform.split("/")
        cmd += ["--override-os", os_, "--override-arch", arch]
        cmd += ["--image-parallel-copies", "10"]

    cmd += [SOURCE, TARGET]

    return SOURCE, TARGET, cmd


# =========================================================
# 并发执行 skopeo copy
# =========================================================
async def sync_one(line: str, duplicates: Dict[str, bool]):
    SOURCE, TARGET, cmd = build_skopeo_cmd(line, duplicates)

    print(f"\n[SYNCHRONIZING] {line}")
    print(f"  Source: {SOURCE}")
    print(f"  Target: {TARGET}")
    print(f"  Command: {' '.join(cmd)}")

    rc, out, err = await run_cmd(cmd)

    if rc == 0:
        print(f"✅ SUCCESS → {TARGET}")
    else:
        print(f"❌ FAILED → {TARGET}")
        print(err)

    return rc


# =========================================================
# 主流程
# =========================================================
async def main():
    await docker_login()

    lines = parse_images()
    duplicates = detect_duplicate_images(lines)

    print(f"[INFO] Total images parsed: {len(lines)}")
    print(f"[INFO] Duplicates detected: {list(duplicates.keys())}\n")

    # 限制并发
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    async def sem_task(line):
        async with sem:
            return await sync_one(line, duplicates)

    tasks = [sem_task(line) for line in lines]
    results = await asyncio.gather(*tasks)

    failed = sum(1 for r in results if r != 0)

    print("\n===================================================")
    print("                SUMMARY REPORT")
    print("===================================================")
    print(f"Total: {len(lines)}, Success: {len(lines)-failed}, Failed: {failed}")

    if failed > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
