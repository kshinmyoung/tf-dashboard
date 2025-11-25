import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import date

# ─────────────────────
# 0. 기본 설정
# ─────────────────────

st.set_page_config(page_title="증빙자료 TF 대시보드", layout="wide")

# 구글 시트 ID (문서 주소의 /d/ 와 /edit 사이 값)
SPREADSHEET_ID = st.secrets["SPREADSHEET_ID"]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ─────────────────────
# 1. Google Sheet 연결 함수
# ─────────────────────

@st.cache_resource
def get_gsheet_client():
    """st.secrets에 저장된 서비스 계정으로 gspread 클라이언트 생성"""
    credentials = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPES,
    )
    gc = gspread.authorize(credentials)
    return gc


@st.cache_data(ttl=60)
def load_data():
    """
    구글 시트에서 '증빙자료'가 들어간 시트를 자동으로 찾아서
    DataFrame과 Worksheet 객체를 함께 반환
    (헤더 중복/빈칸도 자동 처리)
    """
    gc = get_gsheet_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    worksheets = sh.worksheets()
    sheet_titles = [ws.title for ws in worksheets]

    # 화면 상단에 참고용으로 시트 목록 보여주기
    st.caption(f"이 문서 안에 있는 시트들: {sheet_titles}")

    # 제목에 '증빙자료'라는 글자가 들어가는 시트 찾기
    target_ws = None
    for ws in worksheets:
        if "증빙자료" in ws.title:
            target_ws = ws
            break

    # 못 찾으면 첫 번째 시트를 사용 (안전장치)
    if target_ws is None:
        target_ws = worksheets[0]
        st.warning(
            f"'증빙자료'라는 글자가 들어간 시트를 찾지 못해 "
            f"첫 번째 시트('{target_ws.title}')를 대신 사용합니다."
        )

    ws = target_ws
    st.caption(f"현재 사용 중인 시트: '{ws.title}'")

    # 시트 내용 전체 읽기 (헤더 포함)
    values = ws.get_all_values()  # [[행1], [행2], ...]
    if not values:
        # 시트가 비어 있는 경우
        return pd.DataFrame(), ws

    raw_header = values[0]
    data_rows = values[1:]

    # 헤더(1행)에 빈칸/중복이 있으면 자동으로 이름 부여
    header = []
    seen = {}
    for idx, h in enumerate(raw_header):
        name = (h or "").strip()
        if name == "":
            name = f"col_{idx+1}"
        base = name
        count = seen.get(base, 0)
        if count > 0:
            # 중복 시 "_2", "_3" 등 붙이기
            name = f"{base}_{count+1}"
        seen[base] = count + 1
        header.append(name)

    df = pd.DataFrame(data_rows, columns=header)

    # 'Unnamed' 로 시작하는 불필요한 열 제거(혹시 모를 엑셀 잔재)
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]

    # 필수 컬럼이 없으면 기본값으로 추가
    for col in ["담당자", "진행상태", "진행률", "자료링크", "마감일", "비고"]:
        if col not in df.columns:
            df[col] = ""

    # 진행률 숫자형 정리 (0~100)
    df["진행률"] = (
        pd.to_numeric(df["진행률"], errors="coerce")
        .fillna(0)
        .clip(0, 100)
        .astype(int)
    )

    # 마감일 날짜형 정리
    df["마감일"] = pd.to_datetime(df["마감일"], errors="coerce")

    # 내부용 row id (저장 시 어떤 행인지 찾기 위한 키)
    df.reset_index(inplace=True)
    df.rename(columns={"index": "_row_id"}, inplace=True)

    return df, ws


# ─────────────────────
# 2. 표시등(빨/노/파) 계산 함수 (복합 규칙 버전)
# ─────────────────────

def calc_indicator(row: pd.Series) -> str:
    """
    표시등 복합 규칙:

    🔴 (위험):
      - 마감일 지났고 진행률 < 100
      - 담당자 없음
      - 진행상태 ∈ ["중단", "이슈", "문제", "보류"]
      - 진행률 <= 30

    🟡 (주의):
      - 마감일까지 7일 이하 남았고 미완료
      - 30 < 진행률 <= 70
      - 진행상태 ∈ ["지연", "늦음"]

    🔵 (정상):
      - 위 조건에 해당하지 않으면 모두 파랑
    """
    today = date.today()
    progress = int(row.get("진행률", 0))
    status = (row.get("진행상태", "") or "").strip()
    owner = (row.get("담당자", "") or "").strip()
    due = row.get("마감일", None)

    # 마감일 처리 (문자열이면 datetime으로 변환 시도)
    if isinstance(due, str):
        try:
            due = pd.to_datetime(due, errors="coerce")
        except Exception:
            due = None

    if isinstance(due, pd.Timestamp):
        due_date = due.date()
    else:
        due_date = None

    # -----------------------
    # 🔴 위험 조건
    # -----------------------
    # 1) 마감일 지남 + 미완료
    if due_date and due_date < today and progress < 100:
        return "🔴"

    # 2) 담당자 없음
    if owner == "":
        return "🔴"

    # 3) 진행상태가 문제/중단/보류
    danger_states = ["중단", "이슈", "문제", "보류"]
    if status in danger_states:
        return "🔴"

    # 4) 진행률 매우 낮음
    if progress <= 30:
        return "🔴"

    # -----------------------
    # 🟡 주의 조건
    # -----------------------
    # 1) 마감일 임박 (7일 이하) + 미완료
    if due_date and 0 <= (due_date - today).days <= 7 and progress < 100:
        return "🟡"

    # 2) 진행률 중간 (30~70)
    if 30 < progress <= 70:
        return "🟡"

    # 3) 진행상태 지연
    warning_states = ["지연", "늦음"]
    if status in warning_states:
        return "🟡"

    # -----------------------
    # 🔵 정상 조건
    # -----------------------
    return "🔵"


# ─────────────────────
# 3. 메인 앱
# ─────────────────────

def main():
    st.title("대학 인증 증빙자료 준비 현황 대시보드")

    # 데이터 불러오기
    df, ws = load_data()

    if df.empty:
        st.warning("증빙자료 시트에 데이터가 없습니다. 구글 시트 내용을 먼저 채워 주세요.")
        return

    # 표시등 계산
    df["표시등"] = df.apply(calc_indicator, axis=1)

    # ── 사이드바 필터 ──
    st.sidebar.header("필터")

    # 평가영역 필터
    if "평가영역" in df.columns:
        areas = ["전체"] + sorted(df["평가영역"].dropna().unique().tolist())
        selected_area = st.sidebar.selectbox("평가영역", areas, index=0)
    else:
        selected_area = "전체"

    # 평가준거 필터
    if "평가준거" in df.columns:
        kriterias = ["전체"] + sorted(df["평가준거"].dropna().unique().tolist())
        selected_krit = st.sidebar.selectbox("평가준거", kriterias, index=0)
    else:
        selected_krit = "전체"

    # 주무부처 필터
    if "주무부처" in df.columns:
        depts = ["전체"] + sorted(df["주무부처"].dropna().unique().tolist())
        selected_dept = st.sidebar.selectbox("주무부처", depts, index=0)
    else:
        selected_dept = "전체"

    # 담당자 필터
    if "담당자" in df.columns:
        owners = ["전체"] + sorted(df["담당자"].dropna().unique().tolist())
        selected_owner = st.sidebar.selectbox("담당자", owners, index=0)
    else:
        selected_owner = "전체"

    # 필터 적용
    filtered = df.copy()
    if selected_area != "전체" and "평가영역" in df.columns:
        filtered = filtered[filtered["평가영역"] == selected_area]
    if selected_krit != "전체" and "평가준거" in df.columns:
        filtered = filtered[filtered["평가준거"] == selected_krit]
    if selected_dept != "전체" and "주무부처" in df.columns:
        filtered = filtered[filtered["주무부처"] == selected_dept]
    if selected_owner != "전체" and "담당자" in df.columns:
        filtered = filtered[filtered["담당자"] == selected_owner]

    # ── 상단 요약 카드 ──
    total = len(filtered)
    done = (filtered["진행률"] == 100).sum()
    red = (filtered["표시등"] == "🔴").sum()
    yellow = (filtered["표시등"] == "🟡").sum()

    # 지연(마감일 경과 미완료) 계산 - datetime 비교 안전하게
    if "마감일" in filtered.columns:
        dates = pd.to_datetime(filtered["마감일"], errors="coerce")
        today_ts = pd.Timestamp.today().normalize()
        overdue = ((dates < today_ts) & (filtered["진행률"] < 100)).sum()
    else:
        overdue = 0

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("전체 증빙 항목", int(total))
    with c2:
        st.metric("제출완료 (100%)", int(done))
    with c3:
        st.metric("위험 (🔴)", int(red))
    with c4:
        st.metric("주의 (🟡)", int(yellow))
    with c5:
        st.metric("지연 (마감일 경과 미완료)", int(overdue))

    st.write("---")

    # ── 편집 가능한 테이블 ──
    st.subheader("증빙자료 리스트 (진행상태/진행률/담당자/비고 등 수정 가능)")

    base_cols = [
        "표시등",
        "평가영역",
        "평가준거",
        "보고서 주요내용",
        "제출자료(예시)",
        "구비서류",
        "주무부처",
        "담당자",
        "진행상태",
        "진행률",
        "자료링크",
        "마감일",
        "비고",
    ]
    # 실제 있는 열만 표시
    show_cols = ["_row_id"] + [c for c in base_cols if c in filtered.columns]
    view_df = filtered[show_cols].copy()

    # ── column_config (진행상태 드롭다운, 진행률 숫자 입력 등) ──
    col_config = {}

    # 내부 row_id는 수정 불가
    if "_row_id" in view_df.columns and hasattr(st.column_config, "NumberColumn"):
        col_config["_row_id"] = st.column_config.NumberColumn(
            "row_id",
            disabled=True,
            width="small",
        )

    # 표시등 읽기 전용
    if "표시등" in view_df.columns and hasattr(st.column_config, "TextColumn"):
        col_config["표시등"] = st.column_config.TextColumn(
            "표시등",
            disabled=True,
            width="small",
        )

    # 진행상태: SelectboxColumn 지원 시 드롭다운으로
    status_options = ["미착수", "진행중", "완료", "보류", "지연"]
    if hasattr(st.column_config, "SelectboxColumn") and "진행상태" in view_df.columns:
        col_config["진행상태"] = st.column_config.SelectboxColumn(
            "진행상태",
            options=status_options,
            help="진행상태를 선택하세요.",
        )

    # 진행률: NumberColumn으로 0~100 정수 입력
    if hasattr(st.column_config, "NumberColumn") and "진행률" in view_df.columns:
        col_config["진행률"] = st.column_config.NumberColumn(
            "진행률(%)",
            min_value=0,
            max_value=100,
            step=10,
            help="0~100 사이의 정수를 입력하세요.",
        )

    # 마감일: DateColumn 사용 (지원 시)
    if hasattr(st.column_config, "DateColumn") and "마감일" in view_df.columns:
        col_config["마감일"] = st.column_config.DateColumn("마감일")

    # 편집 불가능한 열 목록
    disabled_cols = [
        "표시등",
        "평가영역",
        "평가준거",
        "보고서 주요내용",
        "제출자료(예시)",
        "구비서류",
        "주무부처",
    ]
    disabled_cols = [c for c in disabled_cols if c in view_df.columns]

    edited_df = st.data_editor(
        view_df,
        hide_index=True,
        use_container_width=True,
        column_config=col_config if col_config else None,
        disabled=disabled_cols,
        num_rows="fixed",
    )

    st.info(
        "각 셀(진행상태/진행률/담당자/자료링크/마감일/비고 등)을 수정한 후, "
        "반드시 아래 '저장' 버튼을 눌러야 구글 시트에 반영됩니다."
    )

    # ── 저장 버튼 ──
    if st.button("변경 내용 구글 시트에 저장하기"):
        updated = df.copy()

        editable_cols = ["담당자", "진행상태", "진행률", "자료링크", "마감일", "비고"]

        # edited_df의 변경 사항을 _row_id 기준으로 반영
        for _, row in edited_df.iterrows():
            rid = int(row["_row_id"])
            mask = updated["_row_id"] == rid
            for col in editable_cols:
                if col in updated.columns and col in row.index:
                    updated.loc[mask, col] = row[col]

        # 진행률 다시 숫자(0~100)로 정리
        if "진행률" in updated.columns:
            updated["진행률"] = (
                pd.to_numeric(updated["진행률"], errors="coerce")
                .fillna(0)
                .clip(0, 100)
                .astype(int)
            )

        # 마감일을 문자열(YYYY-MM-DD)로 변환 (빈 값은 "")
        if "마감일" in updated.columns:
            updated["마감일"] = (
                pd.to_datetime(updated["마감일"], errors="coerce")
                .dt.strftime("%Y-%m-%d")
                .fillna("")
            )

        # 내부용 컬럼 삭제 후 저장용 DataFrame 생성
        save_df = updated.drop(columns=["_row_id", "표시등"], errors="ignore")

        # 구글 시트에 전체 덮어쓰기
        ws.clear()
        ws.append_row(list(save_df.columns))  # 헤더
        ws.append_rows(save_df.astype(str).values.tolist())

        st.success("구글 시트에 저장되었습니다! (페이지를 새로고침하면 표시등이 갱신됩니다.)")
        st.cache_data.clear()  # 캐시 초기화


if __name__ == "__main__":
    main()
