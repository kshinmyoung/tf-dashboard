import streamlit as st
import pandas as pd
import gspread
import altair as alt
from google.oauth2.service_account import Credentials
from datetime import date

# ─────────────────────
# 0. 기본 설정
# ─────────────────────

st.set_page_config(
    page_title="대학 인증 증빙자료 준비 현황 대시보드",
    layout="wide",
    initial_sidebar_state="expanded",
)

SPREADSHEET_ID = st.secrets["SPREADSHEET_ID"]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]


# ─────────────────────
# 1. Google Sheets 연결 & 데이터 로드 (읽기 전용)
# ─────────────────────

@st.cache_resource
def get_gsheet_client():
    """서비스 계정으로 gspread 클라이언트 생성 (읽기 전용 스코프)"""
    credentials = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPES,
    )
    gc = gspread.authorize(credentials)
    return gc


@st.cache_data(ttl=60)
def load_data():
    """
    구글 시트에서 '증빙자료'라는 글자가 들어간 시트를 찾아
    전체 데이터를 DataFrame으로 반환한다.
    (여기서는 어떤 쓰기도 하지 않음: 완전 읽기 전용)
    """
    gc = get_gsheet_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    worksheets = sh.worksheets()
    sheet_titles = [ws.title for ws in worksheets]

    # '증빙자료' 문자열 포함 시트 우선 사용, 없으면 첫 번째 시트 사용
    target_ws = None
    for ws in worksheets:
        if "증빙자료" in ws.title:
            target_ws = ws
            break

    if target_ws is None:
        target_ws = worksheets[0]

    ws = target_ws

    values = ws.get_all_values()  # [[행1], [행2], ...]
    if not values:
        return pd.DataFrame()

    raw_header = values[0]
    data_rows = values[1:]

    # 헤더(1행)에 빈칸·중복 있으면 자동 이름 부여
    header = []
    seen = {}
    for idx, h in enumerate(raw_header):
        name = (h or "").strip()
        if name == "":
            name = f"col_{idx+1}"
        base = name
        count = seen.get(base, 0)
        if count > 0:
            name = f"{base}_{count+1}"
        seen[base] = count + 1
        header.append(name)

    df = pd.DataFrame(data_rows, columns=header)

    # 필요 컬럼이 없으면 기본 추가
    for col in [
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
    ]:
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

    return df


# ─────────────────────
# 2. 표시등(신호등) 계산 함수
# ─────────────────────

def calc_indicator(row: pd.Series) -> str:
    """
    표시등 규칙:

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

    if isinstance(due, str):
        try:
            due = pd.to_datetime(due, errors="coerce")
        except Exception:
            due = None

    if isinstance(due, pd.Timestamp):
        due_date = due.date()
    else:
        due_date = None

    # 🔴 위험
    if due_date and due_date < today and progress < 100:
        return "🔴"
    if owner == "":
        return "🔴"
    if status in ["중단", "이슈", "문제", "보류"]:
        return "🔴"
    if progress <= 30:
        return "🔴"

    # 🟡 주의
    if due_date and 0 <= (due_date - today).days <= 7 and progress < 100:
        return "🟡"
    if 30 < progress <= 70:
        return "🟡"
    if status in ["지연", "늦음"]:
        return "🟡"

    # 🔵 정상
    return "🔵"


# ─────────────────────
# 3. 공식 보고서 텍스트 생성 함수
# ─────────────────────

def generate_official_report_text(df: pd.DataFrame) -> str:
    """
    필터/정렬된 df를 받아서,
    공식 보고서 형태의 텍스트를 만들어 문자열로 반환.
    (이 문자열을 .txt로 다운로드 → 한글/워드에 붙여넣어 PDF로 저장)
    """
    today_str = pd.Timestamp.today().strftime("%Y-%m-%d")

    total = len(df)
    done = int((df["진행률"] == 100).sum()) if total > 0 else 0
    avg_progress = float(df["진행률"].mean()) if total > 0 else 0.0

    red = int((df["표시등"] == "🔴").sum())
    yellow = int((df["표시등"] == "🟡").sum())
    blue = int((df["표시등"] == "🔵").sum())

    overdue = 0
    due_soon = 0
    if "마감일" in df.columns:
        dates = pd.to_datetime(df["마감일"], errors="coerce")
        today_ts = pd.Timestamp.today().normalize()
        overdue = int(((dates < today_ts) & (df["진행률"] < 100)).sum())
        due_soon = int(
            (
                (dates >= today_ts)
                & (dates <= today_ts + pd.Timedelta(days=7))
                & (df["진행률"] < 100)
            ).sum()
        )

    lines = []
    add = lines.append

    add("───────────────────────────────────────────────")
    add("          [ 대학 인증 증빙자료 준비 현황 보고 ]")
    add("───────────────────────────────────────────────")
    add(f"보고일자: {today_str}")
    add("작성부서: 혁신지원센터 / TF 운영팀")
    add("")
    add("1. 종합 요약 (Executive Summary)")
    add(f"- 전체 증빙 대상 항목: {total}개")
    if total > 0:
        add(f"- 완료된 항목: {done}개 ({(done/total)*100:.1f}%)")
    else:
        add(f"- 완료된 항목: {done}개 (0.0%)")
    add(f"- 평균 진행률: {avg_progress:.1f}%")
    add(f"- 위험(🔴): {red}개 / 주의(🟡): {yellow}개 / 정상(🔵): {blue}개")
    add(f"- 마감 경과(지연) 항목: {overdue}개 / 7일 이내 마감: {due_soon}개")
    add("")

    # 2. 마감 임박/지연
    add("2. 마감 임박 또는 지연 항목 현황")
    if "마감일" in df.columns:
        dates = pd.to_datetime(df["마감일"], errors="coerce")
        today_ts = pd.Timestamp.today().normalize()
        cond = (
            ((dates < today_ts) & (df["진행률"] < 100))
            | (
                (dates >= today_ts)
                & (dates <= today_ts + pd.Timedelta(days=7))
                & (df["진행률"] < 100)
            )
        )
        urgent_df = df[cond].copy()
    else:
        urgent_df = pd.DataFrame([])

    if urgent_df.empty:
        add("- 마감 임박 또는 지연 항목이 없습니다.")
    else:
        add("- 아래 항목은 마감 7일 이내 또는 기한 경과 미완료 항목입니다.")
        add("")
        max_rows = 30
        for idx, (_, row) in enumerate(urgent_df.iterrows()):
            if idx >= max_rows:
                add(f"... (이하 {len(urgent_df) - max_rows}건 생략)")
                break
            area = row.get("평가영역", "")
            crit = row.get("평가준거", "")
            title = row.get("보고서 주요내용", "") or row.get("제출자료(예시)", "")
            title = str(title)[:50]
            owner = row.get("담당자", "")
            prog = row.get("진행률", 0)
            indicator = row.get("표시등", "")
            due = row.get("마감일", "")

            if isinstance(due, pd.Timestamp):
                due_str = due.strftime("%Y-%m-%d")
            else:
                try:
                    due_str = pd.to_datetime(due).strftime("%Y-%m-%d")
                except Exception:
                    due_str = ""

            add(
                f"- [{area}/{crit}] {title} / 담당: {owner} / "
                f"마감: {due_str} / {indicator} {prog}%"
            )
    add("")

    # 3. 평가영역별 진행
    add("3. 평가영역별 진행 현황 요약")
    if "평가영역" in df.columns and total > 0:
        area_progress = (
            df.groupby("평가영역")["진행률"].mean().sort_values(ascending=False)
        )
        for area, val in area_progress.items():
            add(f"- {area}: 평균 진행률 {val:.1f}%")
    else:
        add("- 평가영역 정보가 없습니다.")
    add("")

    # 4. 담당자별 진행
    add("4. 담당자별 진행 현황")
    if "담당자" in df.columns and total > 0:
        by_owner = df.copy()
        by_owner["담당자"] = by_owner["담당자"].fillna("").replace("", "미지정")
        owner_stats = by_owner.groupby("담당자").agg(
            항목수=("진행률", "count"),
            완료수=("진행률", lambda s: int((s == 100).sum())),
            평균진행률=("진행률", "mean"),
        )
        for owner, row in owner_stats.iterrows():
            add(
                f"- {owner}: {row['항목수']}개, "
                f"완료 {row['완료수']}개, 평균 진행률 {row['평균진행률']:.1f}%"
            )
    else:
        add("- 담당자 정보가 없습니다.")
    add("")

    # 5. 액션 아이템
    add("5. 금주 우선 처리 권장 사항")
    add("- 🔴(위험) 항목을 우선적으로 점검하고, 제출자료(예시) 및 자료링크를 보완해 주시기 바랍니다.")
    add("- 마감 7일 이내 항목은 담당부서별로 내부 일정에 반영해 주시기 바랍니다.")
    add("- 담당자 미지정 항목은 조속히 담당자를 지정하여 관리 공백을 줄여주시기 바랍니다.")
    add("")
    add("보고자: 김신명 (TF 사업단장)")
    add("승인: ____________________________")

    return "\n".join(lines)


# ─────────────────────
# 4. 메인 앱 (읽기 전용 UI)
# ─────────────────────

def main():
    st.title("📊 대학 인증 증빙자료 준비 현황 대시보드")

    st.caption(
        "※ 이 화면은 **읽기 전용 대시보드**입니다. "
        "실제 수정(담당자, 진행률, 마감일 등)은 **구글 스프레드시트에서 직접** 해 주세요."
    )

    df = load_data()
    if df.empty:
        st.warning("증빙자료 시트에 데이터가 없습니다. 구글 시트 내용을 먼저 채워 주세요.")
        return

    # 표시등 계산
    df = df.copy()
    df["표시등"] = df.apply(calc_indicator, axis=1)

    # ───── 사이드바 필터 ─────
    st.sidebar.header("🔎 필터")

    # 신호등 범례
    with st.sidebar.expander("신호등 범례", expanded=True):
        st.markdown(
            """
- 🔴 **위험**  
  - 마감일이 지났는데 미완료이거나  
  - 담당자가 비어 있거나  
  - 진행상태가 *중단/이슈/문제/보류* 이거나  
  - 진행률이 30% 이하인 항목

- 🟡 **주의**  
  - 마감일까지 7일 이하 남은 미완료 항목  
  - 진행률이 30~70% 사이  
  - 진행상태가 *지연/늦음* 인 항목

- 🔵 **정상**  
  - 위 조건에 해당하지 않는, 비교적 양호한 항목
"""
        )

    # 평가영역 필터
    if "평가영역" in df.columns:
        areas = ["전체"] + sorted(df["평가영역"].dropna().unique().tolist())
        selected_area = st.sidebar.selectbox("평가영역", areas, index=0)
    else:
        selected_area = "전체"

    # 평가준거
    if "평가준거" in df.columns:
        kriterias = ["전체"] + sorted(df["평가준거"].dropna().unique().tolist())
        selected_krit = st.sidebar.selectbox("평가준거", kriterias, index=0)
    else:
        selected_krit = "전체"

    # 주무부처
    if "주무부처" in df.columns:
        depts = ["전체"] + sorted(df["주무부처"].dropna().unique().tolist())
        selected_dept = st.sidebar.selectbox("주무부처", depts, index=0)
    else:
        selected_dept = "전체"

    # 담당자
    if "담당자" in df.columns:
        owners = (
            df["담당자"]
            .fillna("")
            .astype(str)
            .apply(lambda x: [o.strip() for o in x.replace("/", ",").split(",") if o.strip()])
        )
        flat_owners = sorted(set([o for sub in owners for o in sub]))
        owners_options = ["전체"] + flat_owners
        selected_owner = st.sidebar.selectbox("담당자(이름 포함 검색)", owners_options, index=0)
    else:
        selected_owner = "전체"

    # 신호등 색 필터
    indicator_options = ["전체", "🔴 위험", "🟡 주의", "🔵 정상"]
    selected_indicator = st.sidebar.selectbox("표시등 상태", indicator_options, index=0)

    # 정렬 옵션
    sort_option = st.sidebar.radio(
        "정렬 기준",
        ["위험순 + 마감일순", "마감일 오름차순", "진행률 내림차순"],
        index=0,
    )

    # ── 필터 적용 ──
    filtered = df.copy()

    if selected_area != "전체":
        filtered = filtered[filtered["평가영역"] == selected_area]
    if selected_krit != "전체":
        filtered = filtered[filtered["평가준거"] == selected_krit]
    if selected_dept != "전체":
        filtered = filtered[filtered["주무부처"] == selected_dept]
    if selected_owner != "전체":
        # 담당자 셀 안에 포함된 이름(복수 입력)까지 고려
        mask_owner = filtered["담당자"].fillna("").astype(str).apply(
            lambda x: selected_owner in [o.strip() for o in x.replace("/", ",").split(",")]
        )
        filtered = filtered[mask_owner]
    if selected_indicator != "전체":
        color = selected_indicator.split()[0]  # "🔴 위험" -> "🔴"
        filtered = filtered[filtered["표시등"] == color]

    # 정렬
    filtered = filtered.copy()
    if "마감일" in filtered.columns:
        filtered["마감일"] = pd.to_datetime(filtered["마감일"], errors="coerce")

    if sort_option == "위험순 + 마감일순":
        indicator_rank = {"🔴": 0, "🟡": 1, "🔵": 2}
        filtered["표시등_순위"] = filtered["표시등"].map(indicator_rank).fillna(3)
        filtered = filtered.sort_values(
            by=["표시등_순위", "마감일"],
            ascending=[True, True],
            na_position="last",
        )
    elif sort_option == "마감일 오름차순":
        filtered = filtered.sort_values(
            by=["마감일"],
            ascending=[True],
            na_position="last",
        )
    elif sort_option == "진행률 내림차순":
        filtered = filtered.sort_values(by=["진행률"], ascending=[False])

    # ───── 상단 요약 카드 ─────
    total = len(filtered)
    done = int((filtered["진행률"] == 100).sum())
    red = int((filtered["표시등"] == "🔴").sum())
    yellow = int((filtered["표시등"] == "🟡").sum())
    blue = int((filtered["표시등"] == "🔵").sum())

    if "마감일" in filtered.columns:
        dates = pd.to_datetime(filtered["마감일"], errors="coerce")
        today_ts = pd.Timestamp.today().normalize()
        overdue = int(((dates < today_ts) & (filtered["진행률"] < 100)).sum())
    else:
        overdue = 0

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("전체 증빙 항목", total)
    col2.metric("제출완료 (100%)", done)
    col3.metric("위험 (🔴)", red)
    col4.metric("주의 (🟡)", yellow)
    col5.metric("지연 (마감 경과 미완료)", overdue)

    st.write("---")

    # ───── 탭 구성 ─────
    tab_overview, tab_area, tab_owner, tab_table = st.tabs(
        ["📌 개요", "📚 평가영역별", "👤 담당자별", "📋 상세 목록"]
    )

    # ───── 탭 1: 개요 (신호등 분포 + 전체 진행률 추세) ─────
    with tab_overview:
        st.subheader("신호등 분포")

        indicator_counts = (
            filtered["표시등"]
            .value_counts()
            .reindex(["🔴", "🟡", "🔵"])
            .fillna(0)
            .astype(int)
        )
        ind_df = indicator_counts.reset_index()
        ind_df.columns = ["표시등", "개수"]

        if len(ind_df) > 0:
            chart = (
                alt.Chart(ind_df)
                .mark_bar(radiusTopLeft=4, radiusTopRight=4)
                .encode(
                    x=alt.X("표시등:N", title="신호등"),
                    y=alt.Y("개수:Q", title="항목 수"),
                    tooltip=["표시등", "개수"],
                )
                .properties(height=250)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("표시할 항목이 없습니다. 사이드바 필터를 조정해 보세요.")

        st.subheader("평균 진행률 요약")
        if total > 0:
            avg_progress = float(filtered["진행률"].mean())
            st.progress(avg_progress / 100.0)
            st.write(f"현재 필터 기준 평균 진행률: **{avg_progress:.1f}%**")
        else:
            st.write("현재 필터 조건에 해당하는 항목이 없습니다.")

    # ───── 탭 2: 평가영역별 그래프 ─────
    with tab_area:
        st.subheader("평가영역별 평균 진행률")

        if "평가영역" in filtered.columns and len(filtered) > 0:
            area_progress = (
                filtered.groupby("평가영역")["진행률"]
                .mean()
                .reset_index()
                .rename(columns={"진행률": "평균진행률"})
            )

            area_chart = (
                alt.Chart(area_progress)
                .mark_bar(radiusTopLeft=4, radiusTopRight=4)
                .encode(
                    x=alt.X("평균진행률:Q", title="평균 진행률(%)"),
                    y=alt.Y("평가영역:N", sort="-x", title="평가영역"),
                    tooltip=["평가영역", "평균진행률"],
                )
                .properties(height=300)
            )
            st.altair_chart(area_chart, use_container_width=True)
        else:
            st.info("평가영역 정보가 없거나, 필터 결과가 비어 있습니다.")

    # ───── 탭 3: 담당자별 그래프 ─────
    with tab_owner:
        st.subheader("담당자별 진행 현황")

        if "담당자" in filtered.columns and len(filtered) > 0:
            df_owner = filtered.copy()
            df_owner["담당자"] = (
                df_owner["담당자"]
                .fillna("")
                .replace("", "미지정")
                .astype(str)
            )

            owner_stats = df_owner.groupby("담당자").agg(
                항목수=("진행률", "count"),
                완료수=("진행률", lambda s: int((s == 100).sum())),
                평균진행률=("진행률", "mean"),
            ).reset_index()

            # 평균 진행률 바차트
            st.markdown("**담당자별 평균 진행률**")
            owner_chart = (
                alt.Chart(owner_stats)
                .mark_bar(radiusTopLeft=4, radiusTopRight=4)
                .encode(
                    x=alt.X("평균진행률:Q", title="평균 진행률(%)"),
                    y=alt.Y("담당자:N", sort="-x", title="담당자"),
                    tooltip=["담당자", "항목수", "완료수", "평균진행률"],
                )
                .properties(height=300)
            )
            st.altair_chart(owner_chart, use_container_width=True)

            # 표도 같이 보여주기
            st.markdown("**담당자별 요약 표**")
            st.dataframe(
                owner_stats.sort_values("평균진행률", ascending=False),
                use_container_width=True,
            )
        else:
            st.info("담당자 정보가 없거나, 필터 결과가 비어 있습니다.")

    # ───── 탭 4: 상세 테이블 (조회 전용) ─────
    with tab_table:
        st.subheader("상세 증빙자료 목록 (조회 전용)")

        display_cols = [
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
        display_cols = [c for c in display_cols if c in filtered.columns]

        df_show = filtered[display_cols].copy()

        # 날짜 포맷 보기 좋게
        if "마감일" in df_show.columns:
            df_show["마감일"] = df_show["마감일"].dt.strftime("%Y-%m-%d")

        st.dataframe(df_show, use_container_width=True, height=450)

    st.write("---")

    # ───── 공식 보고서 텍스트 생성 ─────
    st.subheader("📄 공식 보고서 텍스트 생성")

    st.caption(
        "현재 필터/정렬 상태를 기준으로 공식 보고서 텍스트를 생성합니다. "
        "다운로드 후 한글/워드에 붙여넣고, 학교 양식에 맞게 다듬어 사용하시면 됩니다."
    )

    if st.button("📄 TF 공식 보고서(텍스트) 생성"):
        report_text = generate_official_report_text(filtered)
        st.download_button(
            "📥 다운로드: TF_공식보고서.txt",
            report_text.encode("utf-8"),
            file_name="TF_공식보고서.txt",
            mime="text/plain",
        )
        st.text_area("보고서 미리보기", report_text, height=300)


if __name__ == "__main__":
    main()
