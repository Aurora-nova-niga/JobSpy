from __future__ import annotations

import os
import json
import time
import re
import random
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd
import requests
import msal
from openai import OpenAI
from jobspy import scrape_jobs


# -----------------------------
# 0) Config
# -----------------------------

load_dotenv()

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
ASSISTANT_ID = os.environ["OPENAI_ASSISTANT_ID"]  # asst_...
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DB_ID = os.environ["NOTION_DB_ID"]

client = OpenAI(api_key=OPENAI_API_KEY)

CV_FILE_ID = os.environ["OPENAI_CV_FILE_ID"]
CV_VECTOR_STORE_ID = os.environ["OPENAI_CV_VECTOR_STORE_ID"]


# JobSpy抓取参数（你按需改）
JOBSPY_CONFIG = {
    "sites": ["linkedin", "indeed"],
    "search_term": (
        '"supply chain engineer" OR '
        '"logistics engineer" OR '
        '"supply chain analyst" OR '
        '"ingénieur supply chain" OR '
        '"ingénieur logistique" OR '
        '"coordinateur supply chain" OR '
        '"coordinateur logistique" OR '
        '"consultant supply chain" OR '
        '"chef de projet supply chain" OR '
        '"chef de projet logistique" OR '
        '"ingénieur projet"'
    ),
    "location": "Strasbourg, France",
    "hours_old": 72,
    "results_wanted": 10,

    "linkedin_fetch_description": True,
    "description_format": "markdown",
    "verbose": 1,
}

# AI 分析配置
AI_CONFIG = {
    "batch_size": 1,  # 1 = 每次处理 1 个岗位（最可靠）
                      # 0 = 不分批，一次性分析所有岗位
                      # N = 每批 N 个岗位
                      # ⚠️ 由于 Assistant 输出 token 限制，推荐设置为 1
    "max_retries": 3,  # API 调用失败时的最大重试次数（针对 rate limit 等临时错误）
}

# 💡 使用建议：
# 1. 当前设置 batch_size = 1 是因为：
#    - AI Assistant 可能有输出 token 限制（例如 max_tokens 设置太小）
#    - 即使发送 3 个岗位，AI 也只返回 1 个分析
#    - 每次处理 1 个岗位最可靠，虽然慢但确保成功
#
# 2. 如何提高效率（在确保稳定后）：
#    a) 在 OpenAI 平台检查 Assistant 设置：
#       - 找到你的 Assistant (asst_...)
#       - 检查 "Response format" 和 token 限制
#       - 如果有 max_tokens 限制，提高到 4096 或更高
#    b) 修改 Assistant 的 instructions，强调"必须输出数组"
#    c) 然后可以尝试增加 batch_size 到 2 或 3
#
# 3. 如果频繁遇到 "rate_limit_exceeded" 错误：
#    - batch_size = 1 时每次请求间隔 2 秒，通常不会触发
#    - 如果仍有问题，增加 max_retries 到 5
#
# 4. 如果第 3 批出现 "No assistant text message found" 错误：
#    - 可能是 API 临时问题或速率限制
#    - 脚本会自动重试（max_retries = 3）

# Notion字段映射（请按你的 Notion DB 属性名改）
# 你 Notion DB 里建议至少建这些列（名字要一致）：
# Name(title), Company(rich_text), Site(select), Job URL(url), Date Posted(date),
# Score(number), Verdict(select), Reasons(rich_text), Gaps(rich_text), Strategy(rich_text),
# Keywords(rich_text), Risk(select)
NOTION_PROPS = {
    "岗位名称": "岗位名称",          # Title
    "公司名称": "公司名称",          # Rich text
    "招聘平台": "招聘平台",          # Select
    "岗位链接": "岗位链接",          # URL
    "工作地点": "工作地点",          # Rich text
    "发布时间": "发布时间",          # Date
    "合同类型（推断）": "合同类型（推断）",  # Select

    "是否值得投递": "是否值得投递",  # Select
    "匹配评分": "匹配评分",          # Number
    "风险等级": "风险等级",          # Select
    "投递策略": "投递策略",          # Rich text

    "匹配原因": "匹配原因",          # Rich text
    "主要缺口": "主要缺口",          # Rich text
    "关键词": "关键词",              # Multi-select
    "总体建议": "总体建议",          # Rich text

    "原始分析 JSON": "原始分析 JSON",  # Rich text
    "分析日期": "分析日期",            # Date
    "数据来源批次": "数据来源批次",    # Rich text
}



# -----------------------------
# 1) Helpers
# -----------------------------
def extract_json(text: str):
    """
    从 assistant 输出中安全提取 JSON（支持 ```json``` 包裹、前后废话）
    """
    if not text:
        raise ValueError("Empty assistant output")

    original_text = text
    text = text.strip()
    
    print(f"[DEBUG extract_json] 原始输出长度: {len(text)} 字符")
    print(f"[DEBUG extract_json] 前 200 字符: {text[:200]}")

    # 去掉 ```json ... ```
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    # 抓第一个 JSON 数组或对象
    m = re.search(r"(\[[\s\S]*\]|\{[\s\S]*\})", text)
    if not m:
        print(f"[ERROR extract_json] 未找到 JSON 结构")
        print(f"[ERROR extract_json] 完整输出（前 1000 字符）: {original_text[:1000]}")
        raise ValueError(f"Assistant output is not JSON. preview={text[:300]}")

    json_str = m.group(1)
    print(f"[DEBUG extract_json] 提取到的 JSON 长度: {len(json_str)} 字符")
    print(f"[DEBUG extract_json] JSON 开头: {json_str[:100]}")
    
    # 检测是否是数组还是单个对象
    if json_str.strip().startswith('['):
        print(f"[DEBUG extract_json] 检测到 JSON 数组")
    elif json_str.strip().startswith('{'):
        print(f"[WARN extract_json] 检测到单个 JSON 对象（而非数组）")
    
    return json.loads(json_str)
    
def ensure_assistant_has_cv_vector_store(client: OpenAI, assistant_id: str, vector_store_id: str):
    a = client.beta.assistants.retrieve(assistant_id)

    tr = a.tool_resources
    tr_dict = tr.model_dump() if tr else {}
    existing_vs = (tr_dict.get("file_search") or {}).get("vector_store_ids") or []

    if vector_store_id in existing_vs:
        print("[OK] Assistant already linked to CV vector store.")
        return

    tools = list(a.tools or [])
    if not any(
        getattr(t, "type", None) == "file_search" or (isinstance(t, dict) and t.get("type") == "file_search")
        for t in tools
    ):
        tools.append({"type": "file_search"})

    new_vs = list(dict.fromkeys(existing_vs + [vector_store_id]))

    client.beta.assistants.update(
        assistant_id=assistant_id,
        tools=tools,
        tool_resources={"file_search": {"vector_store_ids": new_vs}},
    )
    print("[OK] Assistant updated with CV vector store:", new_vs)

def normalize_results(parsed):
    """
    目标：最终一定返回 List[Dict]
    兼容：
    - parsed 是 str（整段 JSON 作为字符串）
    - parsed 是 list[str]（每个元素是 JSON 字符串）
    - parsed 是 dict（包了一层或多层）
    """
    
    print(f"[DEBUG normalize_results] 输入类型: {type(parsed)}")

    # 1) 如果整体是字符串：再 loads 一次
    if isinstance(parsed, str):
        try:
            parsed = json.loads(parsed)
            print(f"[DEBUG normalize_results] 字符串解析后类型: {type(parsed)}")
        except Exception as e:
            raise ValueError(f"Parsed is a string but not JSON: {parsed[:200]}") from e

    # 2) 如果是 list[str]：逐个 loads
    if isinstance(parsed, list) and (len(parsed) == 0 or isinstance(parsed[0], str)):
        new_list = []
        for i, s in enumerate(parsed):
            if not isinstance(s, str):
                raise ValueError(f"Mixed list types at index {i}: {type(s)}")
            try:
                new_list.append(json.loads(s))
            except Exception as e:
                raise ValueError(f"List item {i} is not JSON string. preview={s[:200]}") from e
        parsed = new_list
        print(f"[DEBUG normalize_results] list[str] 解析完成，共 {len(parsed)} 项")

    # 3) 如果已经是 list[dict]：直接返回
    if isinstance(parsed, list) and (len(parsed) == 0 or isinstance(parsed[0], dict)):
        print(f"[DEBUG normalize_results] 已是 list[dict]，共 {len(parsed)} 项")
        return parsed

    # 4) 如果是 dict：在里面"递归"找到第一个 list[dict]
    if isinstance(parsed, dict):
        print(f"[DEBUG normalize_results] 是 dict，keys: {list(parsed.keys())[:10]}")

        def find_list_of_dict(obj, depth=0, max_depth=6):
            if depth > max_depth:
                return None

            # 直接命中：list[dict]
            if isinstance(obj, list) and obj and all(isinstance(x, dict) for x in obj):
                return obj

            # 空列表也算有效结果（可能没有岗位）
            if isinstance(obj, list) and len(obj) == 0:
                return obj

            if isinstance(obj, dict):
                # 先优先常见 key
                for k in ("results", "data", "items", "output", "content", "analysis", "jobs"):
                    if k in obj:
                        got = find_list_of_dict(obj[k], depth + 1, max_depth)
                        if got is not None:
                            return got
                # 再遍历所有 value
                for v in obj.values():
                    got = find_list_of_dict(v, depth + 1, max_depth)
                    if got is not None:
                        return got
            return None

        found = find_list_of_dict(parsed)
        if found is not None:
            print(f"[DEBUG normalize_results] 在 dict 中找到 list[dict]，共 {len(found)} 项")
            return found

        # 如果 dict 本身就是一个岗位对象（AI 只返回了 1 个对象而不是数组）
        if "job_url" in parsed:
            print(f"[WARN normalize_results] AI 只返回了单个 dict 对象，将其包装成数组")
            return [parsed]

        raise ValueError(f"Dict parsed but cannot find list[dict]. keys={list(parsed.keys())[:20]}")

    raise ValueError(f"Expected list[dict], got {type(parsed)}")

def fetch_jobs() -> pd.DataFrame:
    jobs = scrape_jobs(
        site_name=JOBSPY_CONFIG["sites"],   # 这里传 list，让 jobspy 自己多站点抓
        search_term=JOBSPY_CONFIG["search_term"],
        location=JOBSPY_CONFIG["location"],
        results_wanted=JOBSPY_CONFIG["results_wanted"],
        hours_old=JOBSPY_CONFIG["hours_old"],
        linkedin_fetch_description=JOBSPY_CONFIG.get("linkedin_fetch_description", False),
        description_format=JOBSPY_CONFIG.get("description_format", "markdown"),
        verbose=JOBSPY_CONFIG.get("verbose", 1),
    )

    keep_cols = [
        "id", "site", "title", "company", "location", "date_posted", "job_url",
        "job_url_direct", "description", "job_type", "job_level", "company_industry"
    ]
    for c in keep_cols:
        if c not in jobs.columns:
            jobs[c] = None
    return jobs[keep_cols].copy()


def jobs_df_to_payload(jobs: pd.DataFrame) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for _, row in jobs.iterrows():
        records.append({
            "job_id": row.get("id"),
            "site": row.get("site"),
            "title": row.get("title"),
            "company": row.get("company"),
            "location": row.get("location"),
            "date_posted": str(row.get("date_posted")) if row.get("date_posted") else None,
            "job_url": row.get("job_url"),
            "job_url_direct": row.get("job_url_direct"),
            "job_level": row.get("job_level"),
            "company_industry": row.get("company_industry"),
            "description": row.get("description") or "",
        })
    return records


def build_system_instructions() -> str:
    """
    构建系统指令（只需发送一次）
    """
    return """
你是一名欧洲（法国）技术岗位招聘分析专家，
专注于以下领域：
- Supply Chain / Logistics Engineering
- Project Management Consulting / Technical PM

你的任务是分析岗位列表并输出 JSON 数组。
候选人的完整简历 **仅通过 file_search 工具提供**（CV 已入库并绑定到你）。

【输出格式（严格要求）】
- 必须输出 JSON 数组格式：[{...}, {...}, ...]
- 即使只有 1 个岗位，也必须输出数组：[{...}]
- 禁止输出单个对象 {...}
- 必须用方括号 [] 包裹

【输出长度控制】
- 禁止长段落、禁止重复解释
- 每个字段保持简洁

【检索要求】
- 你必须使用 file_search 检索候选人简历，再进行匹配与判断
- 不允许凭空假设候选人经历
- 若简历里找不到关键信息，必须在 gaps 中明确写"简历未体现：xxx"

【分析步骤】（在内部思考，但不要输出思考过程）
1. 抽取岗位关键信息(职位名称，公司名，薪资，地区，职责，技能要求，语言要求，期望的候选人，合同类型等)
2. 判断岗位真实性与清晰度
3. 与候选人简历进行匹配
4. 给出明确的"是否值得投递"的结论
5. 输出严格结构化 JSON

【匹配规则】
- 匹配度超过60%，即为"投"
- 40%到60%之间，即为"谨慎投"
- 低于40%，即为"不投"

【每个岗位输出 Schema】
{
  "job_url": "...",
  "job_title": "...",
  "company": "...",
  "location": "...",
  "contract_type_guess": "CDI|CDD|Freelance|Unknown|Reject",
  "salary_guess_eur_month_gross": null,
  "risk_flag": "low|medium|high",
  "score": 0,
  "verdict": "投|不投|谨慎投",
  "match_reasons": ["..."],
  "gaps": ["..."],
  "keywords": ["..."],
  "apply_strategy": "...",
  "overall_advice": "..."
}

我已经理解了要求。现在请发送岗位数据，我会按照上述格式分析。
""".strip()


def build_batch_prompt(jobs_payload: List[Dict[str, Any]], batch_num: int = None) -> str:
    """
    构建单批岗位数据的 prompt（在同一个对话中使用）
    """
    batch_info = f"（批次 {batch_num}）" if batch_num else ""
    return f"""
请分析以下 {len(jobs_payload)} 个岗位{batch_info}，输出 JSON 数组 [{{"..."}}, {{"..."}}, ...]:

{json.dumps(jobs_payload, ensure_ascii=False, indent=2)}

⚠️ 记住：必须输出包含 {len(jobs_payload)} 个元素的 JSON 数组！
""".strip()


def build_user_prompt(jobs_payload: List[Dict[str, Any]]) -> str:
    """
    构建精简版 prompt，减少 token 消耗
    """
    # 特别处理：如果只有 1 个岗位，也要强调输出数组格式
    count = len(jobs_payload)
    format_note = f"输出 JSON 数组 [{count} 个元素]，即使只有 1 个也用 [{{'...'}}]"
    
    return f"""
分析 {count} 个法国供应链/物流工程岗位，判断是否值得投递。

输出：JSON 数组 [{{...}}, {{...}}, ...]，包含 {count} 个元素
使用 file_search 检索候选人 CV 进行匹配（简历已绑定到你）

每个岗位输出：
{{
  "job_url": "...",
  "job_title": "...",
  "company": "...",
  "location": "...",
  "contract_type_guess": "CDI|CDD|Freelance|Unknown",
  "salary_guess_eur_month_gross": null,
  "risk_flag": "low|medium|high",
  "score": 0-100,
  "verdict": "投|谨慎投|不投",
  "match_reasons": ["简短原因1", "简短原因2"],
  "gaps": ["简历缺少xxx"],
  "keywords": ["关键词"],
  "apply_strategy": "1-2句建议",
  "overall_advice": "1句话总结"
}}

评分规则：>60投，40-60谨慎投，<40不投

岗位数据（{count} 个）：
{json.dumps(jobs_payload, ensure_ascii=False)}

⚠️ 必须输出 {count} 个分析！格式：[{{...}}, {{...}}]
""".strip()

def build_daily_report_prompt(results: list[dict], today: str) -> str:
    # 统计数据
    verdict_count = {"投": 0, "谨慎投": 0, "不投": 0}
    top_jobs = []
    
    for r in results:
        verdict = r.get("verdict", "谨慎投")
        verdict_count[verdict] = verdict_count.get(verdict, 0) + 1
        if verdict == "投":
            top_jobs.append(r)
    
    # 按评分排序，取前3
    top_jobs.sort(key=lambda x: x.get("score", 0), reverse=True)
    top_jobs = top_jobs[:3]
    
    return f"""
⚠️ 重要：这是一个写邮件的任务，不是数据分析任务！

你的任务：写一封给用户看的【今日岗位小报告】邮件。
输出要求：纯文本、自然语言、人类可直接阅读。
绝对禁止：不要输出 JSON、不要输出代码、不要输出任何 {{"key": "value"}} 格式。

--------------------
邮件正文要求：
--------------------

标题行：📌 今日岗位小报告 | {today}

第一段：用一句话总结今天的岗位情况
例如："今天为你分析了 {len(results)} 个供应链相关岗位，其中 {verdict_count.get('投', 0)} 个值得投递，{verdict_count.get('谨慎投', 0)} 个建议谨慎投递，{verdict_count.get('不投', 0)} 个不建议投递。"

第二部分：🌟 最值得投的 Top 3
列出 3 个最推荐的岗位，每个岗位包含：
- 岗位名称 | 公司 | 地点
- 推荐原因（1-2句话）
- 下一步行动建议（1句话）
- 岗位链接

第三部分：⚠️ 谨慎投递提醒
如果有谨慎投的岗位，用 1-2 段话总结共性问题。

第四部分：❌ 不建议投递的原因
如果有不建议投的岗位，用 1 段话总结原因。

第五部分：📝 今日行动清单
列出 3-6 条具体可执行的建议，例如：
1. 定制简历，突出某某经验
2. 补充某某能力
3. ...

结尾：一句鼓励的话。

--------------------
现在开始写邮件正文（直接开始，不要任何前置说明）：

参考数据：
今天日期：{today}
总岗位数：{len(results)}
值得投：{verdict_count.get('投', 0)} 个
谨慎投：{verdict_count.get('谨慎投', 0)} 个
不建议投：{verdict_count.get('不投', 0)} 个

Top 3 岗位：
{json.dumps(top_jobs, ensure_ascii=False, indent=2)}

所有岗位：
{json.dumps(results, ensure_ascii=False, indent=2)}

⚠️ 再次提醒：不要输出 JSON！直接写邮件正文！从标题"📌 今日岗位小报告"开始！
""".strip()


# -----------------------------
# 2) OpenAI Assistant call (batch)
# -----------------------------
def run_assistant_in_thread(
    client: OpenAI,
    thread_id: str,
    assistant_id: str,
    message_content: str,
    expected_jobs: Optional[int] = None,
    max_retries: int = 3,
) -> tuple[List[Dict[str, Any]], str]:
    """
    在已有的 thread 中发送消息并获取响应
    """
    for attempt in range(max_retries):
        try:
            # 在已有 thread 中添加消息
            client.beta.threads.messages.create(
                thread_id=thread_id,
                role="user",
                content=message_content
            )

            run = client.beta.threads.runs.create(
                thread_id=thread_id,
                assistant_id=assistant_id,
            )

            while True:
                run = client.beta.threads.runs.retrieve(
                    thread_id=thread_id,
                    run_id=run.id
                )
                if run.status in ("completed", "failed", "cancelled", "expired"):
                    break
                time.sleep(1.2)

            # 处理 rate limit 错误，自动重试
            if run.status == "failed" and run.last_error:
                error_code = run.last_error.code if hasattr(run.last_error, 'code') else str(run.last_error)
                error_msg = run.last_error.message if hasattr(run.last_error, 'message') else str(run.last_error)
                
                if error_code == "rate_limit_exceeded":
                    # 从错误消息中提取等待时间
                    import re
                    wait_match = re.search(r"try again in ([\d.]+)s", error_msg)
                    wait_time = float(wait_match.group(1)) if wait_match else 15
                    
                    if attempt < max_retries - 1:
                        print(f"[WARN] 遇到速率限制，等待 {wait_time:.1f} 秒后重试... (尝试 {attempt+1}/{max_retries})")
                        time.sleep(wait_time + 2)
                        continue
                    else:
                        raise RuntimeError(f"达到最大重试次数 ({max_retries})，速率限制错误: {error_msg}")
                else:
                    raise RuntimeError(f"Run failed: status={run.status}, error={error_code}: {error_msg}")
            
            if run.status != "completed":
                raise RuntimeError(f"Run not completed: status={run.status}, last_error={run.last_error}")
            
            # 记录 run 的 token 使用情况
            if hasattr(run, 'usage') and run.usage:
                print(f"[DEBUG] Token 使用: prompt={run.usage.prompt_tokens}, "
                      f"completion={run.usage.completion_tokens}, "
                      f"total={run.usage.total_tokens}")
            
            # 成功完成，跳出重试循环
            break
            
        except RuntimeError as e:
            error_str = str(e).lower()
            
            # 可重试的错误类型
            if "rate_limit" in error_str or "no assistant text message found" in error_str:
                if attempt < max_retries - 1:
                    wait_time = 5 + attempt * 2
                    print(f"[WARN] 遇到错误: {str(e)[:100]}")
                    print(f"[WARN] 等待 {wait_time} 秒后重试... (尝试 {attempt+1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"[ERROR] 达到最大重试次数 ({max_retries})，错误: {e}")
                    raise
            else:
                # 其他类型错误，直接抛出
                raise

    msgs = client.beta.threads.messages.list(thread_id=thread_id)

    print(f"[DEBUG] Thread 中共有 {len(msgs.data)} 条消息")
    
    assistant_text = None
    for m in reversed(msgs.data):
        print(f"[DEBUG] 消息角色: {m.role}, content blocks: {len(m.content)}")
        if m.role != "assistant":
            continue
        text = ""
        for block in m.content:
            print(f"[DEBUG]   Block type: {block.type}")
            if block.type == "text":
                text += block.text.value
            if text.strip():
                assistant_text = text
                break  # 只取最新的一条
        if assistant_text:
            break
    
    if not assistant_text:
        print(f"[ERROR] Run 状态: {run.status}")
        print(f"[ERROR] Run usage: {run.usage if hasattr(run, 'usage') else 'N/A'}")
        print(f"[ERROR] 未找到 assistant 文本消息！")
        print(f"[ERROR] Thread ID: {thread_id}, Run ID: {run.id}")
        
        for i, m in enumerate(msgs.data):
            print(f"[ERROR] Message {i}: role={m.role}, content_count={len(m.content)}")
            if m.role == "assistant":
                for j, block in enumerate(m.content):
                    print(f"[ERROR]   Block {j}: type={block.type}")
                    if block.type == "text":
                        print(f"[ERROR]   Text preview: {block.text.value[:200] if hasattr(block.text, 'value') else 'N/A'}")
        
        raise RuntimeError(f"No assistant text message found. Thread: {thread_id}, Run: {run.id}")

    print(f"[AI] 收到原始输出，长度: {len(assistant_text)} 字符")

    parsed = extract_json(assistant_text)
    results = normalize_results(parsed)
    if expected_jobs is not None and len(results) != expected_jobs:
        print(f"[WARN] 期望 {expected_jobs} 条分析，实际得到 {len(results)} 条")
    
    return results, assistant_text


def run_assistant_analysis(
    client: OpenAI,
    assistant_id: str,
    user_prompt: str,
    expected_jobs: Optional[int] = None,
    max_retries: int = 3,
) -> tuple[List[Dict[str, Any]], str]:
    """
    返回: (results: List[Dict], raw_text: str)
    支持自动重试（rate limit 错误）
    """
    for attempt in range(max_retries):
        try:
            thread = client.beta.threads.create(
                messages=[{
                    "role": "user",
                    "content": user_prompt
                }]
            )

            run = client.beta.threads.runs.create(
                thread_id=thread.id,
                assistant_id=assistant_id,
            )

            while True:
                run = client.beta.threads.runs.retrieve(
                    thread_id=thread.id,
                    run_id=run.id
                )
                if run.status in ("completed", "failed", "cancelled", "expired"):
                    break
                time.sleep(1.2)

            # 处理 rate limit 错误，自动重试
            if run.status == "failed" and run.last_error:
                error_code = run.last_error.code if hasattr(run.last_error, 'code') else str(run.last_error)
                error_msg = run.last_error.message if hasattr(run.last_error, 'message') else str(run.last_error)
                
                if error_code == "rate_limit_exceeded":
                    # 从错误消息中提取等待时间
                    import re
                    wait_match = re.search(r"try again in ([\d.]+)s", error_msg)
                    wait_time = float(wait_match.group(1)) if wait_match else 15
                    
                    if attempt < max_retries - 1:
                        print(f"[WARN] 遇到速率限制，等待 {wait_time:.1f} 秒后重试... (尝试 {attempt+1}/{max_retries})")
                        time.sleep(wait_time + 2)  # 多等 2 秒确保安全
                        continue
                    else:
                        raise RuntimeError(f"达到最大重试次数 ({max_retries})，速率限制错误: {error_msg}")
                else:
                    raise RuntimeError(f"Run failed: status={run.status}, error={error_code}: {error_msg}")
            
            if run.status != "completed":
                raise RuntimeError(f"Run not completed: status={run.status}, last_error={run.last_error}")
            
            # 记录 run 的 token 使用情况
            if hasattr(run, 'usage') and run.usage:
                print(f"[DEBUG] Token 使用: prompt={run.usage.prompt_tokens}, "
                      f"completion={run.usage.completion_tokens}, "
                      f"total={run.usage.total_tokens}")
            
            # 成功完成，跳出重试循环
            break
            
        except RuntimeError as e:
            error_str = str(e).lower()
            
            # 可重试的错误类型
            if "rate_limit" in error_str or "no assistant text message found" in error_str:
                if attempt < max_retries - 1:
                    wait_time = 5 + attempt * 2  # 逐渐增加等待时间：5s, 7s, 9s
                    print(f"[WARN] 遇到错误: {str(e)[:100]}")
                    print(f"[WARN] 等待 {wait_time} 秒后重试... (尝试 {attempt+1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"[ERROR] 达到最大重试次数 ({max_retries})，错误: {e}")
                    raise
            else:
                # 其他类型错误，直接抛出
                raise

    msgs = client.beta.threads.messages.list(thread_id=thread.id)

    print(f"[DEBUG] Thread 中共有 {len(msgs.data)} 条消息")
    
    assistant_text = None
    for m in reversed(msgs.data):  # 从最早到最新扫，最后一个 assistant_text 会是最新
        print(f"[DEBUG] 消息角色: {m.role}, content blocks: {len(m.content)}")
        if m.role != "assistant":
            continue
        text = ""
        for block in m.content:
            print(f"[DEBUG]   Block type: {block.type}")
            if block.type == "text":
                text += block.text.value
            if text.strip():
                assistant_text = text  # 不 return，继续，让它被最新的覆盖
    
    if not assistant_text:
        print(f"[ERROR] Run 状态: {run.status}")
        print(f"[ERROR] Run usage: {run.usage if hasattr(run, 'usage') else 'N/A'}")
        print(f"[ERROR] 未找到 assistant 文本消息！")
        print(f"[ERROR] Thread ID: {thread.id}, Run ID: {run.id}")
        
        # 尝试获取更多信息
        for i, m in enumerate(msgs.data):
            print(f"[ERROR] Message {i}: role={m.role}, content_count={len(m.content)}")
            if m.role == "assistant":
                for j, block in enumerate(m.content):
                    print(f"[ERROR]   Block {j}: type={block.type}")
                    if block.type == "text":
                        print(f"[ERROR]   Text preview: {block.text.value[:200] if hasattr(block.text, 'value') else 'N/A'}")
        
        raise RuntimeError(f"No assistant text message found. Thread: {thread.id}, Run: {run.id}")

    print(f"[AI] 收到原始输出，长度: {len(assistant_text)} 字符")

    parsed = extract_json(assistant_text)
    results = normalize_results(parsed)
    if expected_jobs is not None and len(results) != expected_jobs:
        print(f"[WARN] 期望 {expected_jobs} 条分析，实际得到 {len(results)} 条")
    
    return results, assistant_text


def generate_fallback_report(results: list[dict], today: str) -> str:
    """
    当 AI 生成报告失败时，生成备用简化报告
    """
    verdict_count = {"投": 0, "谨慎投": 0, "不投": 0}
    top_jobs = []
    
    for r in results:
        verdict = r.get("verdict", "谨慎投")
        verdict_count[verdict] = verdict_count.get(verdict, 0) + 1
        if verdict == "投":
            top_jobs.append(r)
    
    # 按评分排序
    top_jobs.sort(key=lambda x: x.get("score", 0), reverse=True)
    
    lines = []
    lines.append("=" * 60)
    lines.append(f"📌 今日岗位小报告 | {today}")
    lines.append("=" * 60)
    lines.append("")
    lines.append(f"今天为你分析了 {len(results)} 个供应链相关岗位：")
    lines.append(f"• 值得投递：{verdict_count['投']} 个")
    lines.append(f"• 谨慎投递：{verdict_count['谨慎投']} 个")
    lines.append(f"• 不建议投递：{verdict_count['不投']} 个")
    lines.append("")
    lines.append("-" * 60)
    lines.append("")
    lines.append("🌟 最值得投的岗位")
    lines.append("")
    
    for i, job in enumerate(top_jobs[:3], 1):
        lines.append(f"{i}. {job.get('job_title', '未知岗位')} | {job.get('company', '未知公司')} | {job.get('location', '未知地点')}")
        lines.append(f"   评分: {job.get('score', 0)}")
        
        match_reasons = job.get('match_reasons', [])
        if match_reasons and len(match_reasons) > 0:
            lines.append(f"   推荐原因: {match_reasons[0]}")
        
        lines.append(f"   链接: {job.get('job_url', '')}")
        lines.append("")
    
    lines.append("-" * 60)
    lines.append("")
    lines.append("📝 建议")
    lines.append("")
    lines.append("1. 优先关注评分较高的岗位")
    lines.append("2. 定制简历，突出匹配的技能和经验")
    lines.append("3. 准备针对性的 Cover Letter")
    lines.append("")
    lines.append("=" * 60)
    lines.append("💬 祝你求职顺利！")
    lines.append("=" * 60)
    
    return "\n".join(lines)


def convert_json_report_to_text(json_or_text: str, results: list[dict], today: str) -> str:
    """
    如果 AI 返回的是 JSON 格式，将其转换为人类可读的文本报告。
    如果已经是文本格式，直接返回。
    """
    text = json_or_text.strip()
    
    # 检查是否是 JSON 格式
    if text.startswith('{') or (text.startswith('```') and 'json' in text[:20].lower()):
        print("[WARN] AI 返回了 JSON 格式，正在自动转换为美化文本...")
        
        # 去掉 markdown 代码块
        text = re.sub(r'^```(?:json)?\s*', '', text)
        text = re.sub(r'\s*```$', '', text)
        
        try:
            data = json.loads(text)
        except:
            # 如果解析失败，返回原文本
            print("[WARN] JSON 解析失败，返回原始文本")
            return json_or_text
        
        # 手动构建美化的文本报告
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append(f"📌 今日岗位小报告 | {today}")
        report_lines.append("=" * 60)
        report_lines.append("")
        
        # 总览
        overview = data.get("2️⃣ 今日岗位总览", data.get("今日岗位总览", ""))
        if overview:
            report_lines.append(overview)
        else:
            verdict_count = {"投": 0, "谨慎投": 0, "不投": 0}
            for r in results:
                verdict = r.get("verdict", "谨慎投")
                verdict_count[verdict] = verdict_count.get(verdict, 0) + 1
            report_lines.append(f"今天为你分析了 {len(results)} 个供应链相关岗位，"
                              f"其中 {verdict_count['投']} 个值得投递，"
                              f"{verdict_count['谨慎投']} 个建议谨慎投递，"
                              f"{verdict_count['不投']} 个不建议投递。")
        report_lines.append("")
        report_lines.append("-" * 60)
        
        # Top 3
        report_lines.append("")
        report_lines.append("🌟 最值得投的 Top 3")
        report_lines.append("")
        top3 = data.get("3️⃣ 🌟 最值得投的 Top 3", data.get("最值得投的 Top 3", ""))
        if top3:
            # 处理换行，确保格式整齐
            report_lines.append(top3.strip())
        report_lines.append("")
        report_lines.append("-" * 60)
        
        # 谨慎投提醒
        report_lines.append("")
        report_lines.append("⚠️ 谨慎投递提醒")
        report_lines.append("")
        caution = data.get("4️⃣ ⚠️ 谨慎投的岗位共性提醒", data.get("谨慎投的岗位共性提醒", ""))
        if caution:
            report_lines.append(caution.strip())
        else:
            report_lines.append("本次分析的谨慎投递岗位需要特别注意行业背景和语言能力要求。")
        report_lines.append("")
        report_lines.append("-" * 60)
        
        # 不建议投
        report_lines.append("")
        report_lines.append("❌ 不建议投递的原因")
        report_lines.append("")
        no_apply = data.get("5️⃣ ❌ 不建议投的主要原因总结", data.get("不建议投的主要原因总结", ""))
        if no_apply:
            report_lines.append(no_apply.strip())
        else:
            report_lines.append("部分岗位因行业门槛或经验要求与当前背景差距较大，建议优先关注匹配度更高的机会。")
        report_lines.append("")
        report_lines.append("-" * 60)
        
        # 行动清单
        report_lines.append("")
        report_lines.append("📝 今日行动清单")
        report_lines.append("")
        actions = data.get("6️⃣ 📝 今天的行动清单", data.get("今天的行动清单", ""))
        if actions:
            if isinstance(actions, str):
                # 如果是字符串，按换行或编号分割
                action_lines = actions.strip().split('\n')
                for line in action_lines:
                    line = line.strip()
                    if line:
                        # 如果已经有编号，直接用；否则添加编号
                        if re.match(r'^\d+[\.\)、]', line):
                            report_lines.append(line)
                        else:
                            report_lines.append(f"• {line}")
            elif isinstance(actions, list):
                for i, action in enumerate(actions, 1):
                    report_lines.append(f"{i}. {action}")
        else:
            report_lines.append("1. 定制简历，突出核心技能和项目经验")
            report_lines.append("2. 关注最匹配岗位的公司动态")
            report_lines.append("3. 准备针对性的 Cover Letter")
        report_lines.append("")
        report_lines.append("=" * 60)
        
        # 结尾
        report_lines.append("")
        ending = data.get("7️⃣ 💬 结尾一句简短提醒", data.get("结尾一句简短提醒", ""))
        if ending:
            report_lines.append(f"💬 {ending.strip()}")
        else:
            report_lines.append("💬 祝你求职顺利！抓住核心匹配岗位，提升每次投递的转化率。")
        report_lines.append("")
        report_lines.append("=" * 60)
        
        print("[OK] JSON 已成功转换为美化文本格式")
        return "\n".join(report_lines)
    
    # 已经是文本格式
    return json_or_text


def run_daily_report_text(client: OpenAI, assistant_id: str, results: List[Dict[str, Any]]) -> str:
    """
    生成每日岗位小报告
    """
    print("[报告] 开始生成每日岗位小报告...")
    today = datetime.now().strftime("%Y-%m-%d")
    prompt = build_daily_report_prompt(results, today)
    
    print(f"[报告] Prompt 长度: {len(prompt)} 字符")

    try:
        thread = client.beta.threads.create(messages=[{"role": "user", "content": prompt}])
        print(f"[报告] Thread 创建成功: {thread.id}")
        
        run = client.beta.threads.runs.create(thread_id=thread.id, assistant_id=assistant_id)
        print(f"[报告] Run 创建成功: {run.id}")

        while True:
            run = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
            if run.status in ("completed", "failed", "cancelled", "expired"):
                break
            time.sleep(1.2)

        print(f"[报告] Run 状态: {run.status}")
        
        if run.status != "completed":
            error_msg = f"status={run.status}, last_error={run.last_error}"
            print(f"[ERROR 报告] {error_msg}")
            raise RuntimeError(f"Daily report run failed: {error_msg}")

        msgs = client.beta.threads.messages.list(thread_id=thread.id)
        print(f"[报告] Thread 中共有 {len(msgs.data)} 条消息")

        # 拿最新一条 assistant 文本
        assistant_text = None
        for m in reversed(msgs.data):
            print(f"[报告] 消息角色: {m.role}, content blocks: {len(m.content)}")
            if m.role != "assistant":
                continue
            text = ""
            for block in m.content:
                print(f"[报告]   Block type: {block.type}")
                if block.type == "text":
                    text += block.text.value
            if text.strip():
                assistant_text = text
                print(f"[报告] 找到 assistant 文本，长度: {len(text)} 字符")
                break

        if not assistant_text:
            print(f"[ERROR 报告] 未找到 assistant 文本消息！")
            print(f"[ERROR 报告] Thread ID: {thread.id}, Run ID: {run.id}")
            
            # 尝试从所有消息中找到任何文本
            for i, m in enumerate(msgs.data):
                print(f"[ERROR 报告] Message {i}: role={m.role}")
                for j, block in enumerate(m.content):
                    if hasattr(block, 'text') and hasattr(block.text, 'value'):
                        print(f"[ERROR 报告]   Text preview: {block.text.value[:200]}")
            
            # 生成备用报告
            print("[报告] 生成备用简化报告...")
            return generate_fallback_report(results, today)

        print(f"[报告] AI 返回文本前 200 字符: {assistant_text[:200]}")
        
        # 如果是 JSON 格式，转换为文本
        final_text = convert_json_report_to_text(assistant_text.strip(), results, today)
        print(f"[报告] 最终报告长度: {len(final_text)} 字符")
        return final_text
        
    except Exception as e:
        print(f"[ERROR 报告] 生成报告时出错: {e}")
        print(f"[报告] 使用备用报告...")
        return generate_fallback_report(results, today)


def text_to_simple_html(text: str) -> str:
    escaped = (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
    )
    # 换行变 <br>
    html = escaped.replace("\n", "<br>")
    return f"<html><body style='font-family:Arial,Helvetica,sans-serif;line-height:1.5'>{html}</body></html>"


# -----------------------------
# Graph 邮件发送（Device Code Flow + /me/sendMail）
# -----------------------------
GRAPH_SCOPES = ["User.Read", "Mail.Send"]  # delegated scopes for Graph
TOKEN_CACHE_PATH = Path("ms_token_cache.bin")


def _load_cache() -> msal.SerializableTokenCache:
    cache = msal.SerializableTokenCache()
    if TOKEN_CACHE_PATH.exists():
        cache.deserialize(TOKEN_CACHE_PATH.read_text(encoding="utf-8"))
    return cache


def _save_cache(cache: msal.SerializableTokenCache) -> None:
    if cache.has_state_changed:
        TOKEN_CACHE_PATH.write_text(cache.serialize(), encoding="utf-8")


def get_graph_access_token() -> str:
    """
    Device Code Flow:
    - 第一次运行：会打印验证码+登录链接，你在浏览器完成登录授权
    - 之后运行：优先走缓存，无需交互（适合 Task Scheduler）
    """
    client_id = os.getenv("MS_CLIENT_ID")
    authority = os.getenv("MS_AUTHORITY", "https://login.microsoftonline.com/consumers")
    if not client_id:
        raise RuntimeError("Missing env var: MS_CLIENT_ID")
    if not authority:
        raise RuntimeError("Missing env var: MS_AUTHORITY")

    cache = _load_cache()
    app = msal.PublicClientApplication(client_id=client_id, authority=authority, token_cache=cache)

    # 1) 先静默取 token（有缓存就不需要登录）
    accounts = app.get_accounts()
    result = None
    if accounts:
        result = app.acquire_token_silent(GRAPH_SCOPES, account=accounts[0])

    # 2) 缓存没有/过期 → 走 device code
    if not result:
        flow = app.initiate_device_flow(scopes=GRAPH_SCOPES)
        if "user_code" not in flow:
            raise RuntimeError(f"Failed to create device flow: {flow}")

        print(flow["message"])  # 会提示去哪个网址输入 code
        result = app.acquire_token_by_device_flow(flow)

    _save_cache(cache)

    if "access_token" not in result:
        raise RuntimeError(f"Could not obtain access token: {result.get('error')} {result.get('error_description')}")
    return result["access_token"]


def send_email_via_graph(subject: str, body_text: str, to_addr: str, body_html: str | None = None) -> None:
    """
    使用 Graph API /me/sendMail 发送邮件
    """
    token = get_graph_access_token()
    url = "https://graph.microsoft.com/v1.0/me/sendMail"

    content_type = "HTML" if body_html else "Text"
    content = body_html if body_html else body_text

    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": content_type, "content": content},
            "toRecipients": [{"emailAddress": {"address": to_addr}}],
        },
        "saveToSentItems": True,
    }

    r = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        data=json.dumps(payload),
        timeout=30,
    )

    if not (200 <= r.status_code < 300):
        raise RuntimeError(f"Graph sendMail failed: {r.status_code} {r.text}")
# -----------------------------
# 3) Notion write
# -----------------------------
def notion_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


def notion_rich_text(s: str) -> Dict[str, Any]:
    return {"rich_text": [{"type": "text", "text": {"content": s[:2000]}}]}


def notion_title(s: str) -> Dict[str, Any]:
    return {"title": [{"type": "text", "text": {"content": s[:2000]}}]}


def notion_select(name: str) -> Dict[str, Any]:
    return {"select": {"name": name}}


def notion_url(u: str) -> Dict[str, Any]:
    return {"url": u}


def notion_number(x: Optional[float]) -> Dict[str, Any]:
    return {"number": x}


def notion_date(d) -> Dict[str, Any]:
    """
    Notion date.start 需要 ISO 8601，如 '2025-12-19'
    兼容 pandas NaN / datetime / 'nan' / '2025-12-19 00:00:00' 等
    """
    if d is None:
        return {"date": None}

    # pandas NaN
    try:
        if pd.isna(d):
            return {"date": None}
    except Exception:
        pass

    s = str(d).strip()
    if not s or s.lower() == "nan":
        return {"date": None}

    # 只取日期部分：'YYYY-MM-DD'
    # 兼容 '2025-12-19 00:00:00' / '2025-12-19T...' / '2025-12-19'
    date_part = s[:10]
    return {"date": {"start": date_part}}


def notion_multi_select(values: list[str]) -> dict:
    # Notion multi_select: [{"name": "xxx"}, ...]
    vals = []
    for v in values or []:
        if not v:
            continue
        vals.append({"name": str(v)[:100]})
    return {"multi_select": vals}
def notion_query_database(filter_obj: Dict[str, Any], page_size: int = 5) -> Dict[str, Any]:
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    payload = {"filter": filter_obj, "page_size": page_size}
    r = requests.post(url, headers=notion_headers(), data=json.dumps(payload), timeout=20)
    if not (200 <= r.status_code < 300):
        raise RuntimeError(f"Notion query failed: {r.status_code} {r.text}")
    return r.json()

def notion_page_exists_by_job_url(job_url: str) -> bool:
    if not job_url:
        return False
    filter_obj = {
        "property": NOTION_PROPS["岗位链接"],
        "url": {"equals": job_url}
    }
    data = notion_query_database(filter_obj, page_size=1)
    return len(data.get("results", [])) > 0

def notion_page_exists_by_title_company_location(job_title: str, company: str, location: str) -> bool:
    job_title = (job_title or "").strip()
    company = (company or "").strip()
    location = (location or "").strip()
    if not job_title or not company:
        return False

    filter_obj = {
        "and": [
            {"property": NOTION_PROPS["岗位名称"], "title": {"equals": job_title}},
            {"property": NOTION_PROPS["公司名称"], "rich_text": {"equals": company}},
            {"property": NOTION_PROPS["工作地点"], "rich_text": {"equals": location}},
        ]
    }
    data = notion_query_database(filter_obj, page_size=1)
    return len(data.get("results", [])) > 0

def create_notion_page(item: Dict[str, Any], batch_tag: str, platform_default: str = "linkedin") -> None:
    """
    item: assistant 输出的每条岗位 JSON
    batch_tag: 本次跑批标识，比如 20251219_1041
    """

    # 兼容：assistant 输出 key 可能是 job_title/company/location，也可能是 title/company/location
    job_title = item.get("job_title") or item.get("title") or "未知岗位"
    company = item.get("company") or item.get("company_name") or ""
    location = item.get("location") or ""
    job_url = item.get("job_url") or ""
    date_posted = item.get("date_posted")  # 你在 main() 里已经 merge 过

    if job_url and notion_page_exists_by_job_url(job_url):
        print(f"[Notion] Skip duplicate (job_url): {job_url}")
        return

    # 2) 再用 title+company+location 兜底（防止某些平台 job_url 不稳定）
    if notion_page_exists_by_title_company_location(job_title, company, location):
        print(f"[Notion] Skip duplicate (title+company+location): {job_title} | {company} | {location}")
        return

    contract_guess = item.get("contract_type_guess") or "Unknown"
    verdict = item.get("verdict") or "谨慎投"
    risk = item.get("risk_flag") or "medium"
    score = item.get("score")
    apply_strategy = item.get("apply_strategy") or ""
    overall_advice = item.get("overall_advice") or ""

    match_reasons = item.get("match_reasons") or []
    gaps = item.get("gaps") or []
    keywords = item.get("keywords") or []

    # 招聘平台：优先用 item 里的 site，否则用默认 linkedin
    site = item.get("site") or platform_default

    props: Dict[str, Any] = {}
    
    # Title
    props[NOTION_PROPS["岗位名称"]] = notion_title(job_title)

    # 基础信息
    props[NOTION_PROPS["公司名称"]] = notion_rich_text(company)
    props[NOTION_PROPS["招聘平台"]] = notion_select(site)
    props[NOTION_PROPS["岗位链接"]] = notion_url(job_url)
    props[NOTION_PROPS["工作地点"]] = notion_rich_text(location)
    props[NOTION_PROPS["发布时间"]] = notion_date(date_posted)
    props[NOTION_PROPS["合同类型（推断）"]] = notion_select(contract_guess)

    # 决策区
    # score 可能是 int/float/str，做一次安全转换
    try:
        score_num = float(score) if score is not None else None
    except:
        score_num = None
    props[NOTION_PROPS["匹配评分"]] = notion_number(score_num)
    props[NOTION_PROPS["是否值得投递"]] = notion_select(verdict)
    props[NOTION_PROPS["风险等级"]] = notion_select(risk)
    props[NOTION_PROPS["投递策略"]] = notion_rich_text(apply_strategy)

    # 分析说明
    props[NOTION_PROPS["匹配原因"]] = notion_rich_text("；".join([str(x) for x in match_reasons])[:2000])
    props[NOTION_PROPS["主要缺口"]] = notion_rich_text("；".join([str(x) for x in gaps])[:2000])
    props[NOTION_PROPS["关键词"]] = notion_multi_select([str(x) for x in keywords])
    props[NOTION_PROPS["总体建议"]] = notion_rich_text(overall_advice[:2000])

    # 留档
    props[NOTION_PROPS["原始分析 JSON"]] = notion_rich_text(json.dumps(item, ensure_ascii=False)[:1900])
    props[NOTION_PROPS["分析日期"]] = {"date": {"start": datetime.now().strftime("%Y-%m-%d")}}
    props[NOTION_PROPS["数据来源批次"]] = notion_rich_text(batch_tag)

    payload = {"parent": {"database_id": NOTION_DB_ID}, "properties": props}

    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=notion_headers(),
        data=json.dumps(payload),
        timeout=20,
    )
    if not (200 <= r.status_code < 300):
        raise RuntimeError(f"Notion create page failed: {r.status_code} {r.text}")



# -----------------------------
# 4) Main
# -----------------------------
def main():
    client = OpenAI(api_key=OPENAI_API_KEY)
    
    # 统计信息字典
    stats = {
        "jobspy_found": 0,
        "sent_to_ai": 0,
        "ai_received": 0,
        "ai_analyzed": 0,
        "notion_written": 0,
        "batch_mode": AI_CONFIG.get("batch_size", 0) > 0,
        "batch_size": AI_CONFIG.get("batch_size", 0),
        "batches_processed": 0,
        "batches_failed": 0,
        "max_retries": AI_CONFIG.get("max_retries", 3),
        "timestamp": datetime.now().strftime("%Y%m%d_%H%M"),
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    # =========================
    # 1️⃣ JobSpy 抓取
    # =========================
    jobs_df = fetch_jobs()
    stats["jobspy_found"] = len(jobs_df)
    
    print(f"[JobSpy] 搜索到 {stats['jobspy_found']} 个岗位")
    print(
        f"[JobSpy] 空描述数量: "
        f"{(jobs_df['description'].fillna('').str.len() == 0).sum()}/{len(jobs_df)}"
    )

    if jobs_df.empty:
        print("[WARN] 未找到任何岗位，退出程序")
        return

    ensure_assistant_has_cv_vector_store(
        client=client,
        assistant_id=ASSISTANT_ID,
        vector_store_id=CV_VECTOR_STORE_ID,
    )

    jobs_payload = jobs_df_to_payload(jobs_df)
    stats["sent_to_ai"] = len(jobs_payload)
    print(f"[准备] 发送给 AI {stats['sent_to_ai']} 条岗位数据")
    
    # 诊断：检查 jobs_payload
    print(f"[DEBUG] jobs_payload 前 3 个岗位的 job_url:")
    for i, job in enumerate(jobs_payload[:3]):
        print(f"  [{i+1}] {job.get('job_url', 'NO URL')}")

    # =========================
    # 2️⃣ AI 分析
    # =========================
    batch_size = AI_CONFIG.get("batch_size", 0)
    
    if batch_size > 0 and len(jobs_payload) > batch_size:
        # 分批处理（每批独立 thread，避免对话历史累积）
        print(f"[AI] 分批处理模式：每批 {batch_size} 个岗位，共 {len(jobs_payload)} 个")
        
        results = []
        assistant_raw_texts = []
        failed_batches = []
        
        # 保存所有批次的数据（调试用）
        all_batches_file = f"batch_data_{stats['timestamp']}.txt"
        with open(all_batches_file, "w", encoding="utf-8") as f:
            f.write("=" * 80 + "\n")
            f.write(f"分批处理：共 {len(jobs_payload)} 个岗位，每批 {batch_size} 个\n")
            f.write("每批使用独立 thread，避免对话历史累积\n")
            f.write("=" * 80 + "\n\n")
        
        total_batches = (len(jobs_payload) + batch_size - 1) // batch_size
        
        # 分批处理，每批创建新 thread
        for i in range(0, len(jobs_payload), batch_size):
            batch = jobs_payload[i:i+batch_size]
            batch_num = i // batch_size + 1
            
            print(f"[AI] 处理批次 {batch_num}/{total_batches}（{len(batch)} 个岗位）")
            
            # 使用精简的 prompt（去掉冗余内容）
            user_prompt = build_user_prompt(batch)
            
            # 保存本批次的完整数据
            with open(all_batches_file, "a", encoding="utf-8") as f:
                f.write(f"\n{'='*80}\n")
                f.write(f"批次 {batch_num}/{total_batches}（{len(batch)} 个岗位）\n")
                f.write(f"{'='*80}\n\n")
                f.write(f"【发送的 Prompt】\n")
                f.write(f"长度: {len(user_prompt)} 字符\n")
                f.write("-" * 80 + "\n")
                f.write(user_prompt)
                f.write("\n\n")
            
            try:
                batch_results, batch_raw_text = run_assistant_analysis(
                    client=client,
                    assistant_id=ASSISTANT_ID,
                    user_prompt=user_prompt,
                    expected_jobs=len(batch),
                    max_retries=AI_CONFIG.get("max_retries", 3),
                )
                
                results.extend(batch_results)
                assistant_raw_texts.append(f"\n{'='*60}\n批次 {batch_num}/{total_batches}\n{'='*60}\n{batch_raw_text}")
                
                print(f"[AI] 批次 {batch_num} 完成，得到 {len(batch_results)} 条结果")
                
                # 保存本批次的 AI 返回结果
                with open(all_batches_file, "a", encoding="utf-8") as f:
                    f.write(f"【AI 返回结果】\n")
                    f.write(f"返回结果数: {len(batch_results)}\n")
                    f.write("-" * 80 + "\n")
                    f.write("原始输出:\n")
                    f.write(batch_raw_text)
                    f.write("\n\n")
                    f.write("解析后的 JSON:\n")
                    f.write(json.dumps(batch_results, ensure_ascii=False, indent=2))
                    f.write("\n\n")
                
            except Exception as e:
                print(f"[ERROR] 批次 {batch_num} 失败: {e}")
                failed_batches.append({
                    "batch_num": batch_num,
                    "jobs": batch,
                    "error": str(e)
                })
                
                # 保存失败信息
                with open(all_batches_file, "a", encoding="utf-8") as f:
                    f.write(f"【批次失败】\n")
                    f.write(f"错误: {str(e)}\n\n")
                
                # 继续处理下一批
            
            # 批次之间稍微等待，避免连续触发 rate limit
            if i + batch_size < len(jobs_payload):
                wait_time = 3 if batch_num < 5 else 5  # 后面的批次等待更长时间
                print(f"[AI] 等待 {wait_time} 秒后处理下一批...")
                time.sleep(wait_time)
        
        assistant_raw_text = "\n\n".join(assistant_raw_texts)
        
        # 在文件末尾添加汇总信息
        with open(all_batches_file, "a", encoding="utf-8") as f:
            f.write("\n" + "=" * 80 + "\n")
            f.write("【所有批次汇总】\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"总批次数: {total_batches}\n")
            f.write(f"成功批次: {total_batches - len(failed_batches)}\n")
            f.write(f"失败批次: {len(failed_batches)}\n")
            f.write(f"总分析结果: {len(results)} 个岗位\n\n")
            
            if len(results) > 0:
                f.write("【所有岗位汇总】\n")
                f.write("-" * 80 + "\n")
                f.write(json.dumps(results, ensure_ascii=False, indent=2))
                f.write("\n\n")
            
            f.write("=" * 80 + "\n")
            f.write("文件结束\n")
            f.write("=" * 80 + "\n")
        
        print(f"[DEBUG] 所有批次数据已保存到: {all_batches_file}")
        print(f"[DEBUG] 文件包含: 完整 Prompt + AI 返回 + 汇总，共 {len(results)} 个分析结果")
        
        # 统计每批的完成情况
        total_batches = (len(jobs_payload) + batch_size - 1) // batch_size
        successful_batches = total_batches - len(failed_batches)
        print(f"[AI] 分批处理完成：处理了 {total_batches} 批，成功 {successful_batches} 批，失败 {len(failed_batches)} 批")
        print(f"[AI] 共得到 {len(results)} 条成功分析结果")
        
        stats["batches_processed"] = total_batches
        stats["batches_failed"] = len(failed_batches)
        
        # 如果有失败的批次，记录下来
        if failed_batches:
            failed_file = f"failed_batches_{stats['timestamp']}.json"
            with open(failed_file, "w", encoding="utf-8") as f:
                json.dump(failed_batches, f, ensure_ascii=False, indent=2)
            print(f"[WARN] 失败批次详情已保存到: {failed_file}")
            print(f"[WARN] 失败的批次编号: {[b['batch_num'] for b in failed_batches]}")
        
    else:
        # 一次性处理所有岗位
        print(f"[AI] 一次性处理 {len(jobs_payload)} 个岗位")
        
        user_prompt = build_user_prompt(jobs_payload)
        
        # 诊断：保存发送给 AI 的 prompt 和 payload
        prompt_debug_file = f"prompt_sent_to_ai_{stats['timestamp']}.txt"
        with open(prompt_debug_file, "w", encoding="utf-8") as f:
            f.write("=" * 80 + "\n")
            f.write("发送给 AI 的完整 Prompt\n")
            f.write("=" * 80 + "\n\n")
            f.write(user_prompt)
            f.write("\n\n" + "=" * 80 + "\n")
            f.write(f"jobs_payload 包含 {len(jobs_payload)} 个岗位\n")
            f.write("=" * 80 + "\n\n")
            f.write(json.dumps(jobs_payload, ensure_ascii=False, indent=2))
        print(f"[DEBUG] Prompt 已保存到: {prompt_debug_file}")
        print(f"[DEBUG] Prompt 长度: {len(user_prompt)} 字符")
        print(f"[DEBUG] Prompt 中包含的岗位数据: {len(jobs_payload)} 条")
        
        results, assistant_raw_text = run_assistant_analysis(
            client=client,
            assistant_id=ASSISTANT_ID,
            user_prompt=user_prompt,
            expected_jobs=len(jobs_payload),
            max_retries=AI_CONFIG.get("max_retries", 3),
        )
        
        stats["batches_processed"] = 1

    stats["ai_received"] = len(results)
    stats["ai_analyzed"] = len([r for r in results if isinstance(r, dict) and r.get("job_url")])
    
    print(f"[AI] AI 返回了 {stats['ai_received']} 条分析结果")
    print(f"[AI] 成功分析 {stats['ai_analyzed']} 个岗位")
    
    # 诊断：检查返回的结果
    if stats['ai_received'] != stats['sent_to_ai']:
        print(f"[WARN] [!] AI 返回数量不匹配！发送了 {stats['sent_to_ai']} 条，但只收到 {stats['ai_received']} 条")
        print(f"[WARN] 这可能是因为：")
        print(f"  1. AI 输出 token 限制：Assistant 的 max_tokens 设置太小")
        print(f"  2. 输入内容太长：AI 无法在单次调用中处理所有岗位")
        print(f"  3. AI 理解错误：AI 可能只分析了第一个岗位")
        print(f"[强烈建议] 启用分批处理模式：在代码中设置 AI_CONFIG['batch_size'] = 3")
        if 'prompt_debug_file' in locals():
            print(f"[调试] 请查看 {prompt_debug_file} 了解发送给 AI 的完整内容")
    
    print(f"[DEBUG] AI 返回的 job_url 列表（前 5 个）:")
    for i, r in enumerate(results[:5]):
        if isinstance(r, dict):
            print(f"  [{i+1}] {r.get('job_url', 'NO URL')}")

    # =========================
    # 3️⃣ 对齐 JobSpy ↔ AI
    # =========================
    jobs_by_url = {j["job_url"]: j for j in jobs_payload if j.get("job_url")}
    jobs_urls = list(jobs_by_url.keys())
    res_urls = [r.get("job_url") for r in results if isinstance(r, dict)]

    print("[DEBUG] first 3 job urls:", jobs_urls[:3])
    print("[DEBUG] result urls:", res_urls)

    missing = [u for u in jobs_urls if u not in set(res_urls)]
    print("[DEBUG] missing urls:", missing)

    # merge 回 date_posted / site
    for item in results:
        j = jobs_by_url.get(item.get("job_url"), {})
        item["date_posted"] = j.get("date_posted")
        item["site"] = j.get("site")

    # =========================
    # 4️⃣ 写入 Notion
    # =========================
    batch_tag = stats["timestamp"]
    notion_success = 0
    for item in results:
        try:
            create_notion_page(
                item,
                batch_tag=batch_tag,
                platform_default="linkedin"
            )
            notion_success += 1
        except Exception as e:
            print(f"[Notion] 写入失败: {item.get('job_url', 'unknown')}, 错误: {e}")

    stats["notion_written"] = notion_success
    print(f"[Notion] 成功写入 {stats['notion_written']}/{len(results)} 条记录到 Notion")

    # =========================
    # 5️⃣ 生成并发送"今日岗位小报告"
    # =========================
    print("\n" + "=" * 60)
    print("【生成每日报告】")
    print("=" * 60)
    
    try:
        report_text = run_daily_report_text(client, ASSISTANT_ID, results)
        print("[报告] [OK] 报告生成成功")
    except Exception as e:
        print(f"[报告] [WARN] AI 报告生成失败: {e}")
        print("[报告] 使用备用简化报告...")
        today = datetime.now().strftime("%Y-%m-%d")
        report_text = generate_fallback_report(results, today)
        print("[报告] [OK] 备用报告生成成功")

    # =========================
    # 6️⃣ 整合所有输出到一个文件
    # =========================
    consolidated_output = f"consolidated_report_{stats['timestamp']}.txt"
    
    with open(consolidated_output, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("JobSpy 自动化岗位分析 - 综合报告\n")
        f.write("=" * 80 + "\n\n")
        
        # 统计信息
        f.write("【执行统计】\n")
        f.write(f"执行时间: {stats['date']}\n")
        f.write(f"批次标识: {stats['timestamp']}\n")
        f.write(f"JobSpy 搜索到岗位数: {stats['jobspy_found']}\n")
        f.write(f"发送给 AI 的岗位数: {stats['sent_to_ai']}\n")
        f.write(f"AI 处理模式: {'分批处理' if stats['batch_mode'] else '一次性处理'}\n")
        if stats['batch_mode']:
            f.write(f"  - 每批大小: {stats['batch_size']} 个岗位\n")
            f.write(f"  - 处理批次数: {stats.get('batches_processed', 0)}\n")
            if stats.get('batches_failed', 0) > 0:
                f.write(f"  - 失败批次数: {stats['batches_failed']}\n")
        f.write(f"  - 最大重试次数: {stats['max_retries']}\n")
        f.write(f"AI 返回结果数: {stats['ai_received']}\n")
        f.write(f"AI 成功分析岗位数: {stats['ai_analyzed']}\n")
        f.write(f"Notion 成功写入数: {stats['notion_written']}\n")
        f.write("\n" + "=" * 80 + "\n\n")
        
        # 每日报告
        f.write("【今日岗位小报告】\n")
        f.write("-" * 80 + "\n")
        f.write(report_text)
        f.write("\n\n" + "=" * 80 + "\n\n")
        
        # 详细分析结果
        f.write("【详细分析结果 JSON】\n")
        f.write("-" * 80 + "\n")
        f.write(json.dumps(results, ensure_ascii=False, indent=2))
        f.write("\n\n" + "=" * 80 + "\n\n")
        
        # AI 原始输出（供调试）
        f.write("【AI 原始输出（完整）】\n")
        f.write("-" * 80 + "\n")
        f.write(f"输出长度: {len(assistant_raw_text)} 字符\n")
        f.write("-" * 80 + "\n")
        f.write(assistant_raw_text)  # 保存完整输出用于调试
        f.write("\n\n" + "=" * 80 + "\n")
        f.write("报告结束\n")
        f.write("=" * 80 + "\n")

    print(f"\n{'='*60}")
    print(f"【执行完成 - 统计摘要】")
    print(f"{'='*60}")
    print(f"JobSpy 搜索到:     {stats['jobspy_found']} 个岗位")
    print(f"发送给 AI:         {stats['sent_to_ai']} 条")
    if stats['batch_mode']:
        success_rate = ((stats.get('batches_processed', 0) - stats.get('batches_failed', 0)) / 
                       stats.get('batches_processed', 1) * 100) if stats.get('batches_processed', 0) > 0 else 0
        print(f"处理模式:          分批（每批 {stats['batch_size']} 个，"
              f"共 {stats.get('batches_processed', 0)} 批，"
              f"成功率 {success_rate:.1f}%）")
        if stats.get('batches_failed', 0) > 0:
            print(f"                   [!] {stats['batches_failed']} 批失败")
    else:
        print(f"处理模式:          一次性")
    print(f"AI 返回:           {stats['ai_received']} 条")
    print(f"AI 成功分析:       {stats['ai_analyzed']} 条")
    print(f"Notion 写入成功:   {stats['notion_written']} 条")
    print(f"{'='*60}")
    print(f"综合报告已保存: {consolidated_output}")
    print(f"{'='*60}\n")

    # 发邮件（Microsoft Graph）
    print("\n" + "=" * 60)
    print("【发送邮件】")
    print("=" * 60)
    
    to_addr = os.getenv("EMAIL_TO")
    if not to_addr:
        print("[Email] [WARN] 未设置 EMAIL_TO 环境变量，跳过邮件发送")
    else:
        try:
            print(f"[Email] 收件人: {to_addr}")
            subject = f"📌 今日岗位小报告｜{datetime.now():%Y-%m-%d}｜共{len(results)}条"
            print(f"[Email] 主题: {subject}")
            print(f"[Email] 报告长度: {len(report_text)} 字符")
            
            report_html = text_to_simple_html(report_text)
            print(f"[Email] HTML 长度: {len(report_html)} 字符")
            
            send_email_via_graph(subject=subject, body_text=report_text, body_html=report_html, to_addr=to_addr)
            print("[Email] [OK] 邮件已通过 Microsoft Graph 成功发送！")
        except Exception as e:
            print(f"[Email] [WARN] 邮件发送失败: {e}")
            print(f"[Email] 报告已保存到本地文件: {consolidated_output}")
            # 不抛出异常，让程序继续完成
    
    print("=" * 60)
    print("[OK] 所有任务完成！")
    print("=" * 60)

def safe_main():
    try:
        main()
    except Exception as e:
        with open("run_error.log", "a", encoding="utf-8") as f:
            f.write(f"\n[{datetime.now()}] ERROR\n")
            f.write(traceback.format_exc())
        raise

if __name__ == "__main__":
    safe_main()

