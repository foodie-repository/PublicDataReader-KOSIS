import os
import requests
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from urllib.parse import unquote
import xml.etree.ElementTree as ET
import time
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# --- 1. 설정 (Configuration) ---
GOOGLE_CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH', 'credentials.json')
GOOGLE_SHEET_NAME = os.getenv('GOOGLE_SHEET_NAME', '전국 아파트 매매 실거래가_누적')
SERVICE_KEY = os.getenv('PUBLIC_DATA_SERVICE_KEY')
LAWD_CODE_FILE = os.getenv('LAWD_CODE_FILE', 'lawd_code.csv')
BASE_URL = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade'

# !!! 중요: .env 파일에서 MONTHS_TO_FETCH를 설정하거나 여기에서 직접 수정하세요. !!!
# 예: ['202406', '202407'] -> 6월과 7월 데이터를 모두 가져옵니다.
months_env = os.getenv('MONTHS_TO_FETCH', '202506,202507')
MONTHS_TO_FETCH = [m.strip() for m in months_env.split(',')]

# API 키 확인
if not SERVICE_KEY:
    raise ValueError("PUBLIC_DATA_SERVICE_KEY가 설정되지 않았습니다. .env 파일을 확인하세요.")

# --- 2. 함수 정의 (Functions) ---

def get_lawd_codes(filepath):
    """CSV 파일에서 법정동 코드 목록을 읽어옵니다."""
    try:
        df = pd.read_csv(filepath)
        print(f"총 {len(df['code'])}개의 지역 코드를 불러왔습니다.")
        return df['code'].astype(str).tolist()
    except FileNotFoundError:
        print(f"오류: {filepath} 파일을 찾을 수 없습니다.")
        return []

def fetch_data_for_region(lawd_cd, deal_ymd, service_key):
    """(기존과 동일) 특정 지역, 특정 월의 데이터를 API로부터 가져옵니다."""
    # ... (이전 코드와 동일하므로 생략, 그대로 복사해서 사용하세요)
    # ... 이전 답변의 fetch_data_for_region 함수를 여기에 붙여넣으세요 ...
    all_items = []
    page_no = 1
    
    while True:
        params = {
            'serviceKey': unquote(service_key),
            'LAWD_CD': lawd_cd,
            'DEAL_YMD': deal_ymd,
            'pageNo': str(page_no),
            'numOfRows': '1000'
        }
        try:
            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            root = ET.fromstring(response.content)
            result_code = root.find('header/resultCode').text
            if result_code != '00':
                break
            items_element = root.find('body/items')
            if items_element is None or not items_element:
                break
            current_page_items = items_element.findall('item')
            if not current_page_items:
                break
            for item in current_page_items:
                item_dict = {child.tag: child.text for child in item}
                all_items.append(item_dict)
            page_no += 1
            time.sleep(0.1)
        except requests.exceptions.RequestException as e:
            print(f"  [네트워크 오류] 지역코드: {lawd_cd}, 오류: {e}")
            break
    return all_items


def create_unique_id(df):
    """데이터프레임에 고유 ID 열을 생성합니다."""
    # 고유 ID를 생성할 컬럼들
    # API 응답에 따라 컬럼명이 약간씩 다를 수 있으므로 확인 필요
    id_cols = ['거래금액', '년', '월', '일', '전용면적', '지번', '층', '법정동']
    # 실제 존재하는 컬럼만으로 ID 생성
    valid_cols = [col for col in id_cols if col in df.columns]
    
    # 모든 값을 문자열로 변환하여 합침
    df['unique_id'] = df[valid_cols].astype(str).apply(lambda x: '_'.join(x), axis=1)
    return df

def update_sheet_with_deduplication(months_list):
    """전체 프로세스를 실행하는 메인 함수"""
    print("="*50)
    print("전국 아파트 실거래가 누적 업데이트를 시작합니다.")
    print(f"대상 월: {months_list}")
    print("="*50)
    
    # 1. API로 새로운 데이터 가져오기
    lawd_codes = get_lawd_codes(LAWD_CODE_FILE)
    if not lawd_codes: return

    all_new_data = []
    for month in months_list:
        print(f"\n--- {month} 데이터 수집 중 ---")
        for code in lawd_codes:
            region_data = fetch_data_for_region(code, month, SERVICE_KEY)
            if region_data:
                all_new_data.extend(region_data)
    
    if not all_new_data:
        print("\nAPI로부터 수집된 새로운 데이터가 없습니다. 시트를 확인합니다.")
        df_new = pd.DataFrame()
    else:
        df_new = pd.DataFrame(all_new_data)
        # API 응답 필드명을 일관성 있게 변경 (예: dealYear -> 년)
        # 이는 API 명세에 따라 적절히 수정 필요
        rename_dict = {'dealYear': '년', 'dealMonth': '월', 'dealDay': '일',
                       'dealAmount': '거래금액', 'buildYear': '건축년도',
                       'excluUseAr': '전용면적', 'jibun': '지번', 'floor': '층',
                       'dong': '법정동'}
        df_new.rename(columns=rename_dict, inplace=True)

        print(f"\nAPI로부터 총 {len(df_new)}건의 데이터를 수집했습니다.")
        df_new = create_unique_id(df_new)
    
    # 2. 구글 시트에서 기존 데이터 읽기
    print("\n구글 시트에서 기존 데이터를 읽어옵니다...")
    try:
        gc = gspread.service_account(filename=GOOGLE_CREDENTIALS_PATH)
        sh = gc.open(GOOGLE_SHEET_NAME)
        worksheet = sh.get_worksheet(0)
        existing_records = worksheet.get_all_records()
        df_existing = pd.DataFrame(existing_records)
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"경고: '{GOOGLE_SHEET_NAME}' 시트를 찾을 수 없습니다. 새 시트를 생성합니다.")
        df_existing = pd.DataFrame()
    except Exception as e:
        print(f"구글 시트 읽기 중 오류 발생: {e}")
        return

    # 3. 데이터 비교 및 결합
    if not df_existing.empty:
        print(f"기존 데이터 {len(df_existing)}건을 확인했습니다.")
        df_existing = create_unique_id(df_existing)
        
        # 새로 가져온 데이터 중, 기존에 없는 데이터만 필터링
        df_to_add = df_new[~df_new['unique_id'].isin(df_existing['unique_id'])]
        
        if df_to_add.empty:
            print("\n추가할 새로운 거래 데이터가 없습니다. 프로세스를 종료합니다.")
            return
        
        print(f"\n총 {len(df_to_add)}건의 신규 데이터를 확인했습니다. 시트에 추가합니다.")
        
        # 기존 데이터와 신규 데이터를 합침
        df_final = pd.concat([df_existing, df_to_add], ignore_index=True)
    else:
        print("\n시트가 비어있습니다. 수집된 모든 데이터를 새로 입력합니다.")
        df_final = df_new

    # 'unique_id' 컬럼은 시트에 저장할 필요 없으므로 삭제
    if 'unique_id' in df_final.columns:
        df_final.drop(columns=['unique_id'], inplace=True)
        
    # 4. 최종 데이터를 구글 시트에 쓰기
    print("\n기존 시트 내용을 삭제하고, 최종 데이터를 다시 씁니다...")
    worksheet.clear()
    set_with_dataframe(worksheet, df_final, include_index=False, allow_formulas=False)
    print("="*50)
    print("구글 시트 업데이트가 성공적으로 완료되었습니다!")
    print(f"최종 데이터 건수: {len(df_final)}")
    print("="*50)


# --- 3. 메인 실행 로직 ---
if __name__ == '__main__':
    update_sheet_with_deduplication(MONTHS_TO_FETCH)