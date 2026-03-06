"""
나라장터 사전규격공개 (기술용역) 데이터 수집 및 정렬 스크립트
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
API_KEY          = os.environ["NARA_API_KEY"]          # 나라장터 Open API 인증키
TELEGRAM_TOKEN   = os.environ["TELEGRAM_BOT_TOKEN"]    # 텔레그램 Bot Token
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]      # 텔레그램 Chat ID

# ── 상수 ──────────────────────────────────────────────────────
BASE_URL = (
    "http://apis.data.go.kr/1230000/PrePricePublicInfoService"
    "/getPrePricePublicListInfoServc"
)
PAGE_SIZE = 999   # 1회 최대 요청 건수

# 기술용역 업종코드 목록 (조달청 업종분류 기준)
# 필요에 따라 추가/제거 가능
TECH_SERVICE_CODES = [
    "40101",  # 측량·지적
    "40102",  # 지질·지반조사
    "40103",  # 엔지니어링(전기·기계·화학 등)
    "40104",  # 환경·에너지
    "40105",  # 건설사업관리(CM)
    "40201",  # 정보화전략계획(ISP)
    "40202",  # 시스템 구축
    "40203",  # DB 구축
    "40204",  # 유지보수
    "40205",  # 정보보안
    "40301",  # 학술·연구용역
    "40401",  # 건축설계
    "40402",  # 조경설계
]

# Excel 컬럼 한글 매핑
COLUMN_MAP = {
    "bidNtceNo":          "공고번호",
    "bidNtceNm":          "사업명",
    "ntceInsttNm":        "발주기관",
    "dminsttNm":          "수요기관",
    "asignBdgtAmt":       "배정예산액(원)",
    "prearngPrceDcsnMthdNm": "예가방법",
    "indstrytyCd":        "업종코드",
    "indstrytyNm":        "업종명",
    "rcptDt":             "의견접수마감일",
    "pubPurpAreaNm":      "공개목적지역",
    "rgstDt":             "등록일시",
    "linkUrl":            "상세URL",
}


def get_yesterday_range() -> tuple[str, str]:
    """전일 00:00 ~ 23:59 범위 반환 (yyyyMMddHHmm 형식)"""
    yesterday = datetime.now() - timedelta(days=1)
    start = yesterday.strftime("%Y%m%d") + "0000"
    end   = yesterday.strftime("%Y%m%d") + "2359"
    return start, end


def fetch_all_pages(start_dt: str, end_dt: str) -> list[dict]:
    """페이징 처리하여 전체 데이터 수집"""
    all_items = []
    page = 1

    while True:
        params = {
            "serviceKey": API_KEY,
            "pageNo":     page,
            "numOfRows":  PAGE_SIZE,
            "type":       "json",
            "inqryBgnDt": start_dt,
            "inqryEndDt": end_dt,
        }

        try:
            resp = requests.get(BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"API 호출 실패 (page {page}): {e}")
            break

        # 응답 구조 파싱
        try:
            body       = data["response"]["body"]
            total_cnt  = int(body.get("totalCount", 0))
            items      = body.get("items", {})

            # items가 dict 또는 list 케이스 모두 처리
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
            logger.error(f"응답 파싱 오류: {e} / 원본: {data}")
            break

    return all_items


def filter_tech_service(items: list[dict]) -> list[dict]:
    """기술용역 업종코드에 해당하는 항목만 필터링"""
    filtered = [
        item for item in items
        if str(item.get("indstrytyCd", "")).startswith("40")  # 40xxx = 용역 계열
    ]
    logger.info(f"기술용역 필터링: {len(items)}건 → {len(filtered)}건")
    return filtered


def build_dataframe(items: list[dict]) -> pd.DataFrame:
    """수집 데이터를 DataFrame으로 변환 및 정제"""
    if not items:
        return pd.DataFrame()

    df = pd.DataFrame(items)

    # 필요 컬럼만 추출 (없는 컬럼은 빈 값으로)
    for col in COLUMN_MAP:
        if col not in df.columns:
            df[col] = ""

    df = df[list(COLUMN_MAP.keys())].rename(columns=COLUMN_MAP)

    # 예산액 숫자 변환
    df["배정예산액(원)"] = pd.to_numeric(df["배정예산액(원)"], errors="coerce").fillna(0).astype(int)

    # 예산액 내림차순 정렬
    df = df.sort_values("배정예산액(원)", ascending=False).reset_index(drop=True)
    df.index += 1  # 순위 1부터 시작

    # 예산액 읽기 쉽게 포맷
    df["배정예산액(원)"] = df["배정예산액(원)"].apply(lambda x: f"{x:,}")

    return df


def save_excel(df: pd.DataFrame, date_str: str) -> str:
    """Excel 파일로 저장"""
    filename = f"나라장터_기술용역_사전규격_{date_str}.xlsx"
    filepath = f"/tmp/{filename}"

    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="기술용역_사전규격", index=True, index_label="순위")

        # 컬럼 너비 자동 조정
        ws = writer.sheets["기술용역_사전규격"]
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

    logger.info(f"Excel 저장 완료: {filepath}")
    return filepath


def send_telegram(filepath: str, date_str: str, count: int):
    """텔레그램으로 파일 및 요약 메시지 전송"""
    # 1) 텍스트 메시지 먼저 전송
    msg = (
        f"📋 *나라장터 기술용역 사전규격공개*\n"
        f"📅 기준일: {date_str[:4]}-{date_str[4:6]}-{date_str[6:]}\n"
        f"📊 수집건수: *{count}건*\n"
        f"🔽 배정예산액 높은 순으로 정렬"
    )
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
        timeout=15,
    )

    # 2) Excel 파일 전송
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
    start_dt, end_dt = get_yesterday_range()
    date_str = start_dt[:8]  # yyyyMMdd
    logger.info(f"▶ 수집 시작: {date_str} (00:00 ~ 23:59)")

    # 1. API 데이터 수집
    items = fetch_all_pages(start_dt, end_dt)
    logger.info(f"전체 수집: {len(items)}건")

    if not items:
        logger.warning("수집된 데이터가 없습니다.")
        send_no_data_message(date_str)
        return

    # 2. 기술용역 필터링
    tech_items = filter_tech_service(items)

    # 3. DataFrame 변환 및 정렬
    df = build_dataframe(tech_items)
    logger.info(f"최종 데이터: {len(df)}건")

    # 4. Excel 저장
    filepath = save_excel(df, date_str)

    # 5. 텔레그램 발송
    send_telegram(filepath, date_str, len(df))
    logger.info("▶ 완료")


def send_no_data_message(date_str: str):
    msg = (
        f"📋 *나라장터 기술용역 사전규격공개*\n"
        f"📅 기준일: {date_str[:4]}-{date_str[4:6]}-{date_str[6:]}\n"
        f"ℹ️ 해당일 등록 데이터가 없습니다."
    )
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
        timeout=15,
    )


if __name__ == "__main__":
    main()
