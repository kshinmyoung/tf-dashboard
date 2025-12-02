import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import date
from io import BytesIO

st.set_page_config(page_title="증빙자료 TF 대시보드", layout="wide")

SPREADSHEET_ID = st.secrets["SPREADSHEET_ID"]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


@st.cache_resource
def get_gsheet_client():
    credentials = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPES,
    )
    gc = gspread.authorize(credentials)
    return gc


@st.cache_data(ttl=60)
def load_data():
    gc = get_gsheet_client()
    sh = gc.open_by_key(SPREADSHEET_ID)

    worksheets = sh.worksheets()
    sheet_titles = [ws.title for ws in worksheets]
    st.caption(f"이 문서 안에 있는 시트들: {sheet_titles}")

    target_ws = None
    for ws in worksheets:
        if "증빙자료" in ws.title:
            target_ws = ws
            break

    if target_ws is None:
        target_ws = worksheets[0]
        st.warning(
            f"'증빙자료'라는 글자가 들어간 시트를 찾지 못해 "
            f"첫 번째 시트('{target_ws.title}')를 대신 사용합니다."
        )

    ws = target_ws
    st.caption(f"현재 사용 중인 시트: '{ws.title}'")

    values = ws.get_all_values()
    if not values:
        return pd.DataFrame(), ws

    raw_header = values[0]
    data_rows = values[1:]

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
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]

    for col in ["담당자", "진행상태", "진행률", "자료링크", "마감일", "비고", "제출자료(예시)"]:
        if col not in df.columns:
            df[col] = ""

    df["진행률"] = (
        pd.to_numeric(df["진행률"], errors="coerce")
        .fillna(0)
        .clip(0, 100)
        .astype(int)
    )

    df["마감일"] = pd.to_datetime(df["마감일"], errors="coerce")

    df.reset_index(inplace=True)
    df.rename(columns={"index": "_row_id"}, inplace=True)

    return df, ws


def calc_indicator(row: pd.Series) -> str:
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
    danger_states = ["중단", "이슈", "문제", "보류"]
    if status in danger_states:
        return "🔴"
    if progress <= 30:
        return "🔴"

    # 🟡 주의
    if due_date and 0 <= (due_date - today).days <= 7 and progress < 100:
        return "🟡"
    if 30 < progress <= 70:
        return "🟡"
    warning_states = ["지연", "늦음"]
    if status in warning_states:
        return "🟡"

    # 🔵 정상
    return "🔵"


# ─────────────────────
# 공식 보고서 PDF 생성 함수
# ─────────────────────
def generate_official_report(df: pd.DataFrame) -> bytes:
    """
    A4 세로 공식 보고서 PDF 생성.
    현재 필터/정렬된 df 기준으로 통계 + 위험/주의 리스트 요약.
    """
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from reportlab.lib.units import mm

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    # 여백 설정
    left_margin = 20 * mm
    top_margin = height - 20 * mm
    line_height = 6 * mm

    # 날짜/기본 통계 계산
    today_str = pd.Timestamp.today().strftime("%Y-%m-%d")
    total = len(df)
    done = int((df["진행률"] == 100).sum()) if total > 0 else 0
    avg_progress = float(df["진행률"].mean()) if total > 0 else 0.0

    red = int((df["표시등"] == "🔴").sum())
    yellow = int((df["표시등"] == "🟡").sum())
    blue = int((df["표시등"] == "🔵").sum())

    # 마감일 기준
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

    def write_line(text, x, y, font="Helvetica", size=10, bold=False):
        c.setFont("Helvetica-Bold" if bold else font, size)
        c.drawString(x, y, text)

    y = top_margin

    # 제목
    write_line("대학 인증 증빙자료 준비 현황 공식 보고", left_margin, y, size=15, bold=True)
    y -= line_height * 1.5

    write_line(f"보고일자: {today_str}", left_margin, y)
    y -= line_height
    write_line("작성부서: 혁신지원센터 / TF 운영팀", left_margin, y)
    y -= line_height * 2

    # 1. 종합 요약
    write_line("1. 종합 요약 (Executive Summary)", left_margin, y, bold=True)
    y -= line_height

    if total == 0:
        write_line("- 현재 집계된 증빙자료 항목이 없습니다.", left_margin + 5 * mm, y)
        c.showPage()
        c.save()
        buffer.seek(0)
        return buffer.getvalue()

    write_line(f"- 전체 증빙 대상 항목: {total}개", left_margin + 5 * mm, y)
    y -= line_height
    write_line(f"- 완료된 항목: {done}개 ({(done/total)*100:.1f}%)", left_margin + 5 * mm, y)
    y -= line_height
    write_line(f"- 평균 진행률: {avg_progress:.1f}%", left_margin + 5 * mm, y)
    y -= line_height
    write_line(f"- 위험(🔴): {red}개 / 주의(🟡): {yellow}개 / 정상(🔵): {blue}개", left_margin + 5 * mm, y)
    y -= line_height
    write_line(f"- 마감 경과(지연) 항목: {overdue}개 / 7일 이내 마감 항목: {due_soon}개", left_margin + 5 * mm, y)
    y -= line_height * 2

    # 2. 마감 임박 또는 지연 항목
    write_line("2. 마감 임박 또는 지연 항목 현황", left_margin, y, bold=True)
    y -= line_height

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
        write_line("- 마감 임박 또는 지연 항목이 없습니다.", left_margin + 5 * mm, y)
        y -= line_height * 2
    else:
        write_line("- 아래 항목은 마감 7일 이내 또는 기한 경과 미완료 항목입니다.", left_margin + 5 * mm, y)
        y -= line_height

        max_rows = 20
        for idx, (_, row) in enumerate(urgent_df.iterrows()):
            if idx >= max_rows:
                write_line(f"... 외 {len(urgent_df) - max_rows}건", left_margin + 5 * mm, y)
                y -= line_height
                break

            area = row.get("평가영역", "")
            crit = row.get("평가준거", "")
            title = row.get("보고서 주요내용", "") or row.get("제출자료(예시)", "")
            title = str(title)[:40]
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

            text = f"- [{area}/{crit}] {title} / 담당: {owner} / 마감: {due_str} / {indicator} {prog}%"
            if y < 30 * mm:
                c.showPage()
                y = top_margin
            write_line(text, left_margin + 5 * mm, y)
            y -= line_height

        y -= line_height

    # 3. 평가영역별 진행률 요약
    write_line("3. 평가영역별 진행 현황 요약", left_margin, y, bold=True)
    y -= line_height

    if "평가영역" in df.columns:
        area_progress = (
            df.groupby("평가영역")["진행률"].mean().sort_values(ascending=False)
        )
        for area, val in area_progress.items():
            if y < 30 * mm:
                c.showPage()
                y = top_margin
            write_line(f"- {area}: 평균 진행률 {val:.1f}%", left_margin + 5 * mm, y)
            y -= line_height
    else:
        write_line("- 평가영역 정보가 없습니다.", left_margin + 5 * mm, y)
        y -= line_height

    y -= line_height

    # 4. 담당자별 진행 현황
    write_line("4. 담당자별 진행 현황", left_margin, y, bold=True)
    y -= line_height

    if "담당자" in df.columns:
        by_owner = df.copy()
        by_owner["담당자"] = by_owner["담당자"].fillna("").replace("", "미지정")
        owner_stats = by_owner.groupby("담당자").agg(
            항목수=("진행률", "count"),
            완료수=("진행률", lambda s: int((s == 100).sum())),
            평균진행률=("진행률", "mean"),
        )

        for owner, row in owner_stats.iterrows():
            if y < 30 * mm:
                c.showPage()
                y = top_margin
            txt = (
                f"- {owner}: {row['항목수']}개, "
                f"완료 {row['완료수']}개, 평균 진행률 {row['평균진행률']:.1f}%"
            )
            write_line(txt, left_margin + 5 * mm, y)
            y -= line_height
    else:
        write_line("- 담당자 정보가 없습니다.", left_margin + 5 * mm, y)
        y -= line_height

    y -= line_height

    # 5. 비고/Action Items (간단 안내)
    write_line("5. 금주 우선 처리 권장 사항", left_margin, y, bold=True)
    y -= line_height
    write_line(
        "- 🔴(위험) 항목을 우선적으로 점검하고, 제출자료(예시) 및 자료링크를 보완해 주시기 바랍니다.",
        left_margin + 5 * mm,
        y,
    )
    y -= line_height
    write_line(
        "- 마감 7일 이내 항목은 담당부서별로 내부 일정에 반영해 주시기 바랍니다.",
        left_margin + 5 * mm,
        y,
    )
    y -= line_height
    write_line(
        "- 담당자 미지정 항목은 조속히 담당자를 지정하여 관리 공백을 줄여주시기 바랍니다.",
        left_margin + 5 * mm,
        y,
    )
    y -= line_height * 2

    write_line("보고자: 김신명 (TF 사업단장)", left_margin, y)
    y -= line_height
    write_line("승인: ____________________________", left_margin, y)

    c.showPage()
    c.save()
    buffer.seek(0)
    return buffer.getvalue()


def main():
    st.title("대학 인증 증빙자료 준비 현황 대시보드")

    df, ws = load_data()
    if df.empty:
        st.warning("증빙자료 시트에 데이터가 없습니다. 구글 시트 내용을 먼저 채워 주세요.")
        return

    df["표시등"] = df.apply(calc_indicator, axis=1)

    with st.expander("신호등 안내 보기", expanded=True):
        st.markdown(
            """
**신호등 범례**

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

    st.sidebar.header("필터")

    with st.sidebar.expander("담당자 이름 자동 추천(복사해서 사용)", expanded=True):
        st.markdown(
            """
- 김정연  
- 오한태  
- 황보창수  
- 이원직  
- 임규혜  
- 황혜숙  
- 박예린  
- 박재훈  
- 이신형  
- 김신명  
- 기타  

👉 한 칸에 여러 명 입력할 경우 예시  
- `김정연, 오한태`  
- `황보창수 / 이원직 / 기타`
"""
        )

    if "평가영역" in df.columns:
        areas = ["전체"] + sorted(df["평가영역"].dropna().unique().tolist())
        selected_area = st.sidebar.selectbox("평가영역", areas, index=0)
    else:
        selected_area = "전체"

    if "평가준거" in df.columns:
        kriterias = ["전체"] + sorted(df["평가준거"].dropna().unique().tolist())
        selected_krit = st.sidebar.selectbox("평가준거", kriterias, index=0)
    else:
        selected_krit = "전체"

    if "주무부처" in df.columns:
        depts = ["전체"] + sorted(df["주무부처"].dropna().unique().tolist())
        selected_dept = st.sidebar.selectbox("주무부처", depts, index=0)
    else:
        selected_dept = "전체"

    if "담당자" in df.columns:
        owners = ["전체"] + sorted(
            set(
                [
                    o.strip()
                    for o in df["담당자"].dropna().tolist()
                    if str(o).strip() != ""
                ]
            )
        )
        selected_owner = st.sidebar.selectbox("담당자 (정확히 일치)", owners, index=0)
    else:
        selected_owner = "전체"

    filtered = df.copy()
    if selected_area != "전체" and "평가영역" in df.columns:
        filtered = filtered[filtered["평가영역"] == selected_area]
    if selected_krit != "전체" and "평가준거" in df.columns:
        filtered = filtered[filtered["평가준거"] == selected_krit]
    if selected_dept != "전체" and "주무부처" in df.columns:
        filtered = filtered[filtered["주무부처"] == selected_dept]
    if selected_owner != "전체" and "담당자" in df.columns:
        filtered = filtered[filtered["담당자"] == selected_owner]

    indicator_rank = {"🔴": 0, "🟡": 1, "🔵": 2}
    filtered = filtered.copy()
    filtered["표시등_순위"] = filtered["표시등"].map(indicator_rank).fillna(3)

    if "마감일" in filtered.columns:
        filtered["마감일"] = pd.to_datetime(filtered["마감일"], errors="coerce")
        filtered_sorted = filtered.sort_values(
            by=["표시등_순위", "마감일"],
            ascending=[True, True],
            na_position="last",
        )
    else:
        filtered_sorted = filtered.sort_values(
            by=["표시등_순위"],
            ascending=[True],
        )

    total = len(filtered_sorted)
    done = (filtered_sorted["진행률"] == 100).sum()
    red = (filtered_sorted["표시등"] == "🔴").sum()
    yellow = (filtered_sorted["표시등"] == "🟡").sum()

    if "마감일" in filtered_sorted.columns:
        dates = pd.to_datetime(filtered_sorted["마감일"], errors="coerce")
        today_ts = pd.Timestamp.today().normalize()
        overdue = ((dates < today_ts) & (filtered_sorted["진행률"] < 100)).sum()
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

    if "평가영역" in filtered_sorted.columns:
        st.subheader("평가영역별 평균 진행률")
        area_progress = (
            filtered_sorted.groupby("평가영역")["진행률"]
            .mean()
            .sort_values(ascending=False)
        )
        chart_df = area_progress.reset_index()
        chart_df = chart_df.rename(columns={"평가영역": "평가영역", "진행률": "평균 진행률"})
        chart_df = chart_df.set_index("평가영역")
        st.bar_chart(chart_df)

    st.write("---")

    # 📄 공식 보고서 PDF 생성 버튼
    st.subheader("공식 보고서 생성")
    st.caption("※ 현재 필터/정렬 상태를 기준으로 A4 세로형 공식 보고서를 PDF로 생성합니다.")
    if st.button("📄 공식 보고서(PDF) 생성"):
        pdf_bytes = generate_official_report(filtered_sorted)
        st.download_button(
            "📥 다운로드: TF 공식 보고서.pdf",
            pdf_bytes,
            file_name="TF_공식보고서.pdf",
            mime="application/pdf",
        )

    st.write("---")

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

    show_cols = ["_row_id"] + [c for c in base_cols if c in filtered_sorted.columns]
    view_df = filtered_sorted[show_cols].copy()

    col_config = {}

    if "_row_id" in view_df.columns and hasattr(st.column_config, "NumberColumn"):
        col_config["_row_id"] = st.column_config.NumberColumn(
            "row_id",
            disabled=True,
            width="small",
        )

    if "표시등" in view_df.columns and hasattr(st.column_config, "TextColumn"):
        col_config["표시등"] = st.column_config.TextColumn(
            "표시등",
            disabled=True,
            width="small",
        )

    status_options = ["미착수", "진행중", "완료", "보류", "지연"]
    if hasattr(st.column_config, "SelectboxColumn") and "진행상태" in view_df.columns:
        col_config["진행상태"] = st.column_config.SelectboxColumn(
            "진행상태",
            options=status_options,
            help="진행상태를 선택하세요.",
        )

    if hasattr(st.column_config, "NumberColumn") and "진행률" in view_df.columns:
        col_config["진행률"] = st.column_config.NumberColumn(
            "진행률(%)",
            min_value=0,
            max_value=100,
            step=10,
            help="0~100 사이의 정수를 입력하세요.",
        )

    if hasattr(st.column_config, "DateColumn") and "마감일" in view_df.columns:
        col_config["마감일"] = st.column_config.DateColumn("마감일")

    disabled_cols = [
        "표시등",
        "평가영역",
        "평가준거",
        "보고서 주요내용",
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
        "각 셀(진행상태/진행률/담당자/자료링크/제출자료(예시)/마감일/비고 등)을 수정한 후, "
        "반드시 아래 '저장' 버튼을 눌러야 구글 시트에 반영됩니다."
    )

    if st.button("변경 내용 구글 시트에 저장하기"):
        updated = df.copy()

        editable_cols = ["담당자", "진행상태", "진행률", "자료링크", "마감일", "비고", "제출자료(예시)"]

        for _, row in edited_df.iterrows():
            rid = int(row["_row_id"])
            mask = updated["_row_id"] == rid
            for col in editable_cols:
                if col in updated.columns and col in row.index:
                    updated.loc[mask, col] = row[col]

        if "진행률" in updated.columns:
            updated["진행률"] = (
                pd.to_numeric(updated["진행률"], errors="coerce")
                .fillna(0)
                .clip(0, 100)
                .astype(int)
            )

        if "마감일" in updated.columns:
            updated["마감일"] = (
                pd.to_datetime(updated["마감일"], errors="coerce")
                .dt.strftime("%Y-%m-%d")
                .fillna("")
            )

        drop_cols = ["_row_id", "표시등", "표시등_순위"]
        save_df = updated.drop(columns=drop_cols, errors="ignore")

        data_to_write = [save_df.columns.tolist()] + save_df.astype(str).values.tolist()
        ws.update(data_to_write)

        st.cache_data.clear()
        st.success("구글 시트에 저장되었습니다! 화면을 새로고침합니다.")

        try:
            st.rerun()
        except Exception:
            st.experimental_rerun()


if __name__ == "__main__":
    main()
