# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 프로젝트 개요

PublicDataReader-KOSIS는 KOSIS(국가통계포털)에서 대한민국 주택 건설 통계 데이터를 수집, 처리, 시각화하는 Python 프로젝트입니다. PublicDataReader 라이브러리를 활용하여 착공/인허가/준공/미분양 데이터를 ETL 파이프라인으로 처리합니다.

## 개발 명령어

```bash
# 의존성 설치 (uv 패키지 매니저 사용)
uv sync

# Python 스크립트 실행
uv run python 착공/착공.py
uv run python 인허가/인허가.py
uv run python 준공/준공.py
uv run python 미분양/미분양.py
uv run python 미분양/준공_후_미분양.py
uv run python 미분양/미분양_종합.py

# Jupyter 노트북 실행
uv run jupyter notebook 착공/착공.ipynb

# 패키지 관리
uv add {패키지명}
uv remove {패키지명}
```

## 아키텍처

### 모듈 구조
각 모듈(착공, 인허가, 준공, 미분양)은 독립적으로 동작하며, 동일한 ETL 패턴을 따릅니다:
- `{모듈}.py` → 데이터 수집 및 변환 스크립트
- `{모듈}.ipynb` → Plotly 기반 시각화 노트북
- `{모듈}_시도별_YYYYMMDD.csv` → 원본 데이터 (타임스탬프 자동 생성)
- `{모듈}_피벗_전체기간_최종.csv` → 분석용 피벗 테이블

### ETL 파이프라인 함수
모든 Python 스크립트는 다음 함수 구조를 따릅니다:
1. `extract_data()` → KOSIS API에서 데이터 수집
2. `save_to_csv()` → 원본 데이터 저장
3. `create_final_pivot_table()` → 피벗 테이블 변환
4. `display_data_info()` → 수집 통계 출력
5. `main()` → 워크플로우 실행

### KOSIS API 제약사항
- **셀 제한**: 요청당 40,000셀 → 지역/기간별 분할 필요
- **인허가 데이터**: 누적치 제공 → `groupby().diff()`로 월별 실적 변환 필요

### KOSIS 테이블 ID
| 모듈 | 테이블 ID | 데이터 유형 | 기간 |
|------|-----------|-------------|------|
| 착공 | DT_MLTM_5387 | 월별 실적 | 2011.01~ |
| 인허가 | DT_MLTM_1948 | 누적 (차분 필요) | 2007.01~ |
| 준공 | DT_MLTM_5373 | 월별 실적 | 2010.08~ |
| 미분양 | DT_1YL202004E | 월별 스냅샷 | 2007.01~ |
| 준공_후_미분양 | DT_MLTM_5328 | 월별 스냅샷 | 2010.01~ |

## 코드 컨벤션

### 데이터 처리 규칙
- **CSV 인코딩**: `utf-8-sig` (한글 깨짐 방지)
- **날짜 형식**: `YYYY.MM` (월 앞 0 패딩, 예: 2024.01)
- **데이터 형태**: Long format (시점, 시도, 주택유형, 개수)
- **필터링**: 소계/기타/총계 행 제외
- **수치 변환**: 모든 숫자는 정수형으로 변환

### 주택 유형
아파트, 단독, 다가구(가구수 기준), 연립, 다세대

### 지역
17개 광역시/도 + 215개 시군구 단위

## 주요 의존성
- `PublicDataReader` - KOSIS API 래퍼
- `pandas` - 데이터 처리
- `plotly` - 인터랙티브 시각화
- `uv` - 패키지 매니저
