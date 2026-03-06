"""
나라장터 사전규격공개 (기술용역) 데이터 수집 및 정렬 스크립트 v2
- 매일 전일 데이터를 수집하여 Excel로 저장 후 텔레그램 발송
"""

import os
import sys
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ── 환경변수 ──────────────────────────────────────────────────
API_KEY          = os.environ["NARA_API_KEY"]
TELEGRAM_TOKEN   = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# ── 상수 ──────────────────────────────────────────────────────
# ★ 핵심 수정: params 딕셔너리 대신 URL 직접 조합 (인증키 이중인코딩 방지)
BASE_URL = "http://apis.data.go.kr/1230000/PrePricePublicInfoService/getPrePricePublicListInfoServc"
PAGE_SIZE = 999

# Excel 컬럼 한글 매핑
COLUMN_MAP = {
    "bidNtceNo":             "공고번호",
    "bidNtceNm":             "사업명",
    "ntceInsttNm":           "발주기관",
    "dminsttNm":             "수요기관",
    "asignBdgtAmt":          "배정예산액(원)",
    "prearngPrceDcsnMthdNm": "예가방법",
    "indstrytyCd":           "업종코드",
    "indstrytyNm":           "업종명",
    "rcptDt":                "의견접수마감일",
    "rgstDt":                "등록일시",
    "linkUrl":               "상세URL",
}


def get_target_date_range() -> tuple[str, str, str]:
    """전일 00:00 ~ 23:59 범위 반환"""
    # 환경변수 TARGET_DATE 있으면 그 날짜 사용 (수동 실행 테스트용)
    target = os.environ.get("TARGET_DATE", "").strip()
    if target and len(target) == 8:
        base = datetime.strptime(target, "%Y%m%d")
    else:
        base = datetime.now() - timedelta(days=1)

    start = base.strftime("%Y%m%d") + "0000"
    end   = base.strftime("%Y%m%d") + "2359"
    date_str = base.strftime("%Y%m%d")
    return start, end, date_str


def fetch_all_pages(start_dt: str, end_dt: str) -> list[dict]:
    """페이징 처리하여 전체 데이터 수집 (URL 직접 조합으로 인코딩 문제 회피)"""
    all_items = []
    page = 1

    while True:
        # ★ requests params= 대신 URL 문자열 직접 조합
        url = (
            f"{BASE_URL}"
            f"?serviceKey={API_KEY}"
            f"&pageNo={page}"
            f"&numOfRows={PAGE_SIZE}"
            f"&type=json"
            f"&inqryBgnDt={start_dt}"
            f"&inqryEndDt={end_dt}"
        )

        try:
            resp = requests.get(url, timeout=30)
            logger.info(f"  HTTP {resp.status_code} (page {page})")

            if resp.status_code != 200:
                logger.error(f"API 오류 응답: {resp.text[:300]}")
                break

            data = resp.json()

        except Exception as e:
            logger.error(f"API 호출 실패 (page {page}): {e}")
            break

        # 응답 구조 파싱
        try:
            body      = data["response"]["body"]
            total_cnt = int(body.get("totalCount", 0))
            items     = body.get("items", {})

            if isinstance(items, dict):
                row_list = items.get("item", [])
                if isinstance(row_list, dict):
                    row_list = [row_list]
            elif isinstance(items, list):
                row_list = items
            else:
                row_list = []

            all_items.extend(row_list)
            logger.info(f"  page {page}: {len(row_list)}건 수집 (누계 {len(all_items)}/{total_cnt})")

            if len(all_items) >= total_cnt or len(row_list) == 0:
                break
            page += 1

        except (KeyError, TypeError) as e:
            logger.error(f"응답 파싱 오류: {e}")
            logger.error(f"원본 응답: {str(data)[:500]}")
            break

    return all_items


def filter_tech_service(items: list[dict]) -> list[dict]:
    """기술용역 필터링 (업종코드 40xxx 계열)"""
    filtered = [
        item for item in items
        if str(item.get("indstrytyCd", "")).startswith("40")
    ]
    logger.info(f"기술용역 필터링: {len(items)}건 → {len(filtered)}건")
    return filtered


def build_dataframe(items: list[dict]) -> pd.DataFrame:
    """DataFrame 변환 및 예산액 내림차순 정렬"""
    if not items:
        return pd.DataFrame()

    df = pd.DataFrame(items)

    for col in COLUMN_MAP:
        if col not in df.columns:
            df[col] = ""

    df = df[list(COLUMN_MAP.keys())].rename(columns=COLUMN_MAP)

    # 예산액 숫자 변환 후 정렬
    df["배정예산액(원)"] = pd.to_numeric(df["배정예산액(원)"], errors="coerce").fillna(0).astype(int)
    df = df.sort_values("배정예산액(원)", ascending=False).reset_index(drop=True)
    df.index += 1

    # 천단위 콤마 포맷
    df["배정예산액(원)"] = df["배정예산액(원)"].apply(lambda x: f"{x:,}")

    return df


def save_excel(df: pd.DataFrame, date_str: str) -> str:
    """Excel 저장 (헤더 스타일 포함)"""
    filename = f"나라장터_기술용역_사전규격_{date_str}.xlsx"
    filepath = f"/tmp/{filename}"

    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="기술용역_사전규격", index=True, index_label="순위")

        ws = writer.sheets["기술용역_사전규격"]

        # 컬럼 너비 자동 조정
        for col_cells in ws.columns:
            max_len = max((len(str(c.value)) if c.value else 0) for c in col_cells)
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 4, 60)

        # 헤더 스타일
        from openpyxl.styles import PatternFill, Font, Alignment
        header_fill = PatternFill("solid", fgColor="1F4E79")
        header_font = Font(color="FFFFFF", bold=True)
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center")

    logger.info(f"Excel 저장: {filepath}")
    return filepath


def send_telegram_message(text: str):
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
        timeout=15,
    )


def send_telegram_file(filepath: str, date_str: str, count: int):
    """텔레그램 메시지 + 파일 전송"""
    y, m, d = date_str[:4], date_str[4:6], date_str[6:]
    msg = (
        f"📋 *나라장터 기술용역 사전규격공개*\n"
        f"📅 기준일: {y}-{m}-{d}\n"
        f"📊 수집건수: *{count}건*\n"
        f"🔽 배정예산액 높은 순 정렬"
    )
    send_telegram_message(msg)

    with open(filepath, "rb") as f:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument",
            data={"chat_id": TELEGRAM_CHAT_ID},
            files={"document": (os.path.basename(filepath), f)},
            timeout=60,
        )

    if resp.status_code == 200:
        logger.info("텔레그램 전송 성공 ✅")
    else:
        logger.error(f"텔레그램 전송 실패: {resp.text}")


def main():
    start_dt, end_dt, date_str = get_target_date_range()
    logger.info(f"▶ 수집 시작: {date_str} (00:00 ~ 23:59)")

    items = fetch_all_pages(start_dt, end_dt)
    logger.info(f"전체 수집: {len(items)}건")

    if not items:
        logger.warning("수집된 데이터 없음")
        y, m, d = date_str[:4], date_str[4:6], date_str[6:]
        send_telegram_message(
            f"📋 *나라장터 기술용역 사전규격공개*\n"
            f"📅 기준일: {y}-{m}-{d}\n"
            f"ℹ️ 해당일 등록 데이터가 없습니다."
        )
        return

    tech_items = filter_tech_service(items)
    df = build_dataframe(tech_items)
    logger.info(f"최종 데이터: {len(df)}건")

    filepath = save_excel(df, date_str)
    send_telegram_file(filepath, date_str, len(df))
    logger.info("▶ 완료")


if __name__ == "__main__":
    main()
