
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
#from airflow.hooks.S3_hook import S3Hook

import pymysql
import boto3
from io import StringIO
from datetime import datetime

from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from  selenium.webdriver.common.keys  import  Keys
import pandas as pd
from collections import Counter
from konlpy.tag import Hannanum
import logging

from csv import writer
import os
import io

def get_S3_connection():
    AWS_ACCESS_KEY ='AKIA6ODVAO4YY7AFVCF4'
    AWS_SECRET_KEY = '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'
    AWS_DEFAULT_REGION = "ap-northeast-2"

    s3_client = boto3.client(service_name='s3',
                         aws_access_key_id=AWS_ACCESS_KEY,
                         aws_secret_access_key=AWS_SECRET_KEY,
                         region_name=AWS_DEFAULT_REGION)
    return s3_client

def get_RDS_connection():
    host = Variable.get('RDS_HOST_NAME')
    port = 3306
    username = Variable.get('RDS_USERNAME')
    password = Variable.get('RDS_PASSWORD')
    db = 'mydb'

    conn = pymysql.connect(host,user=username, passwd=password, db=db,
                           port=port, charset='utf-8')
    return conn

def jobplanet_login():
    driver = webdriver.Chrome()
    driver.get("https://jobplanet.co.kr/users/sign_in?_nav=gb")
    driver.implicitly_wait(1)
    jobplanet_id = Variable.get('JOBPLANET_ID')
    jobplanet_password = Variable.get('JOBPLANET_PASSWORD')

# 아이디 입력, variable 계정정보 뺼것
    id_input = driver.find_element(By.ID, "user_email")
    ActionChains(driver).send_keys_to_element(id_input, jobplanet_id).perform()
    driver.implicitly_wait(1)

# 비밀번호입력
    pw_input = driver.find_element(By.ID, "user_password")
    ActionChains(driver).send_keys_to_element(pw_input, jobplanet_password).perform()
    driver.implicitly_wait(1)

# 로그인
    login_button = driver.find_element(By.CLASS_NAME,"btn_sign_up")
    ActionChains(driver).click(login_button).perform()
    driver.implicitly_wait(1)

    return driver

# 회사별로 긴 정보 s3에 저장
def create_infomation_s3(**kwargs):
    driver = jobplanet_login()
    companies = kwargs['ti'].xcom_pull(task_ids='load_jumpit_file')
    print(f"Companies from XCom: {companies}")

    file_list = kwargs['ti'].xcom_pull(task_ids='read_s3_file')
    bucket_name = Variable.get('AWS_S3_BUCKET')

    for company in companies:
        # 이미 존재하면 패스
        if f'path/in/s3/bucket/review_infomation_{company}.csv' in file_list:  
            continue
        # 없으면 만듦
        else:
            try:
                # 검색창에 회사이름 친다
                search_company = driver.find_element(By.ID, "search_bar_search_query")
                search_company.send_keys(company)
                search_company.send_keys(Keys.RETURN)
                driver.implicitly_wait(2)

                # 검색 클릭
                driver.find_element(By.CLASS_NAME, "tit").click()
                driver.implicitly_wait(2)
                # 크롤링 시행 바로밑에 있음
                review_data = review_crawling(driver)
                upload_to_s3(review_data, f'path/in/s3/bucket/review_information_{company}.csv', bucket_name)
            except:
                pass


def review_crawling(driver):
    # 이름 다 한글화
    review = [] # 리뷰
    merit = []  # 장점
    demerit = [] # 단점
    interview_summary = []  # 면접 한줄 요약
    interview_question = [] # 면접 질문
    interview_answer = []   # 면접 답변 또는 분위기
    hire_way = []   # 채용 방식
    result_date = []    # 결과 발표 시기

    # 팝업장 제거1 클릭
    try:
        driver.find_element(By.CLASS_NAME,"ab-close-button").click()
        driver.implicitly_wait(4)
    except:
        pass

    # 팝업장 제거2 클릭
    try:
        driver.find_element(By.CLASS_NAME, "btn_close_x_ty1").click()
        driver.implicitly_wait(4)
    except:
        pass

    # 리뷰 갯수 세는거
    try:
        review_count = driver.find_element(By.ID, "viewReviewsTitle").find_element(By.CLASS_NAME, "num").text
        review_count = int(review_count)
    except:
        review_count = 0

    # 리뷰페이지에서 4페이지만 뽑음 , 카운트 페이지마다 새는거로 바꿀것
    for _ in range(4):
        reviews = driver.find_elements(By.CLASS_NAME, "content_wrap")

        for i in reviews:
            try:
                summary = (i.find_element(By.CLASS_NAME, "us_label ")).text
                summary = summary.replace(".", "").replace("\n", " ").replace('\"',"")
                review.append(summary)

                review_detail = i.find_elements(By.CLASS_NAME, "df1")
                merit.append(review_detail[0].text.replace(".", "").replace("\n", " ").replace('\"',""))
                demerit.append(review_detail[1].text.replace(".", "").replace("\n", " ").replace('\"',""))
            except:
                pass

        # 총리뷰 갯수가 5개보다 많다면 다음 페이지 이동,
        review_count = review_count - 5
        if review_count > 0:
            try:
                driver.find_element(By.CLASS_NAME, "btn_pgnext").click()
                driver.implicitly_wait(3)
            except:
                pass
        else:
            break

    # 면접탭으로 넘어감
    selector = "#viewCompaniesMenu > ul > li.viewInterviews > a"
    link = driver.find_element(By.CSS_SELECTOR, selector)
    interview_url = link.get_attribute('href')
    driver.get(interview_url)
    driver.implicitly_wait(2)

    # 팝업 없애기
    try:
        driver.find_element(By.CLASS_NAME, "btn_delete_follow_banner").click()
        driver.implicitly_wait(2)
    except:
        pass

    # 직군 선택버튼
    driver.find_element(By.XPATH, '//*[@id="occupation_select"]').click()
    driver.implicitly_wait(2)

    # 전체 직군 면접 리뷰 갯수 세는거
    interview_count = driver.find_element(By.ID, "viewInterviewsTitle").find_element(By.CLASS_NAME, "num").text
    interview_count = int(interview_count)

    try:
        # 데이터(11912) 직군 선택
        driver.find_element(By.XPATH, "//select/option[@value='11912']").click()
        driver.implicitly_wait(2)

        # 데이터 직군 한정 면접 리뷰 개수
        interview_count = driver.find_element(By.ID, "viewInterviewsTitle").find_elements(By.CLASS_NAME, "num")[1].text
        interview_count = int(interview_count)
    except:
        pass

    try:
        # 데이터 직군이 없을 때 개발 직군을 선택
        driver.find_element(By.XPATH, "//select/option[@value='11600']").click()
        driver.implicitly_wait(2)

        # 개발 직군 한정 면접 리뷰 개수
        interview_count = driver.find_element(By.ID, "viewInterviewsTitle").find_elements(By.CLASS_NAME, "num")[1].text
        interview_count = int(interview_count)
    except:
        pass

    # 면접리뷰 2페이지 추출
    for _ in range(2):
        reviews = driver.find_elements(By.CLASS_NAME, "content_wrap")

        for i in reviews:
            try:
                # 면접 리뷰 수집
                # 면접 리뷰 한 줄로 요약
                review_summary = (i.find_element(By.CLASS_NAME, "us_label ")).text
                #문장 한 줄로 바꾸고 전처리
                review_summary = review_summary.replace(".", "").replace("\n", " ").replace('\"', "")
                # 문장 interview_summary에 추가
                interview_summary.append(review_summary)
            except:
                interview_summary.append("no data")

            try:
                #면접질문이 어떤게 나왔는지 수집
                review_detail = i.find_elements(By.CLASS_NAME, "df1")
                interview_question.append(review_detail[0].text.replace(".", "").replace("\n", " ").replace('\"',""))
            except:
                interview_question.append("no data")

            try:
                #면접직문에 어떤 답을 했는지
                review_detail = i.find_elements(By.CLASS_NAME, "df1")
                interview_answer.append(review_detail[1].text.replace(".", "").replace("\n", " ").replace('\"',""))
            except:
                interview_answer.append("no data")

            try:
                # 면접방식(그룹,1:1면접,ppt)
                review_detail = i.find_elements(By.CLASS_NAME, "df1")
                hire_way.append(review_detail[2].text.replace(".", "").replace("\n", " ").replace('\"',""))
            except:
                hire_way.append("no data")
            
            try:
                # 결과 발표시기
                review_detail = i.find_elements(By.CLASS_NAME, "df1")
                result_date.append(review_detail[3].text.replace(".", "").replace("\n", " ").replace('\"',""))
            except:
                result_date.append("no data")

        # 리뷰 5개 이상 모으면 다음 페이지
        if interview_count > 5:
            try:
                driver.find_element(By.CLASS_NAME, "btn_pgnext").click()
                driver.implicitly_wait(3)
            except:
                pass
        else:
            break

    # 리뷰 없으면 0리턴
    if len(review) == 0 and len(interview_summary) == 0:
        return 0

    # 리뷰,면접리뷰 중 리뷰 갯수가 많으면, 면접리뷰 리스트의 길이를 거기에 맞춤
    if len(review) > len(interview_summary):
        tmp = ["no data"] * (len(review) - len(interview_summary))
        # 리스트+리스트 로 리스트 길이를 맞춤
        interview_summary = interview_summary + tmp
        interview_question = interview_question + tmp
        interview_answer = interview_answer + tmp
        hire_way = hire_way + tmp
        result_date = result_date + tmp
        
     # 리뷰,면접리뷰 중 면접리뷰 갯수가 많으면, 리뷰 리스트의 길이를 거기에 맞춤
    elif len(review) < len(interview_summary):
        tmp = ["no data"] * (len(interview_summary) - len(review))
        review = review + tmp
        merit = merit + tmp
        demerit = demerit + tmp

    # 데이터 프레임 만듦
    result_info = pd.DataFrame({
        'review_summary' : review,
        'merit' : merit,
        'demerit' : demerit,
        'interview_summary' : interview_summary,
        'interview_question' : interview_question,
        'interview_answer' : interview_answer,
        'hire_way' : hire_way,
        'result_date' : result_date,
    })
    return result_info

# s3에 있는 파일을 불러와서 데이터 작업 후 rds에 있는 파일에 넣기
def create_summary_s3_to_rds(**kwargs):
    driver = jobplanet_login()
    companies = kwargs['ti'].xcom_pull(task_ids='load_jumpit_file')
    file_list = kwargs['ti'].xcom_pull(task_ids='read_s3_file')
    bucket_name = Variable.get('AWS_S3_BUCKET')

    if 'path/in/s3/bucket/summary.csv' not in file_list:
        # 요약본 파일이 없는 경우, 정보수집 방법
        try:
            # 정보수집
            review_wordcloud = wordcloud_info(companies[0])
            company_salary_info = create_salary_infomation(driver, companies[0])
        except:
            pass
        entire_summary = pd.DataFrame({
                'company' : [companies[0]],
                'review_summary': [review_wordcloud[0]],
                'merit_summary': [review_wordcloud[1]],
                'demerit_summary': [review_wordcloud[2]],
                'interview_summary': [review_wordcloud[3]],
                'question_summary': [review_wordcloud[4]],
                'answer_summary': [review_wordcloud[5]],
                'average_salary' : [company_salary_info],
            })

        upload_to_s3(entire_summary, 'path/in/s3/bucket/summary.csv', bucket_name)
        companies = companies[1:]

    # 전체 요약본 파일이 rds에 있을 때 정보수집 방법
    # 그 파일을 읽음
    s3_client = get_S3_connection()
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key="path/in/s3/bucket/summary.csv")
        df = pd.read_csv(obj['Body'])
    except Exception as e:
        print(f"Failed to read CSV file from S3: {e}")

    # 이미 요약본 자료가 존재하는 회사들 추출
    summary_company = df['company'].tolist() 
    for company in companies:
        # 그 회사들은 넘긴다.(=요약본에 있는 회사들)
        if company in summary_company:
            continue
        # 존재하지 않는다.(=요약본에 없는 회사들)
        else:
            driver = jobplanet_login()
            try:
                # 워드 클라우드
                review_wordcloud = wordcloud_info(company)
                # 연봉 정보 뽑음
                company_salary_info = create_salary_infomation(driver, company)
                # 회사명과 정보들을 하나의 리스트로 모으기 
                list_info = review_wordcloud + [company_salary_info]
                new_df = pd.DataFrame([list_info])
                merged_df = pd.concat([df, new_df], ignore_index=True)
                csv_buffer = StringIO()
                merged_df.to_csv(csv_buffer, index=False)
                s3_client.put_object(Bucket=bucket_name, Key="path/in/s3/bucket/summary.csv", Body=csv_buffer)
                print("Data appended successfully.")
            except:
                pass

    
# 연봉정보 뽑는 함수
def create_salary_infomation(driver, company):
    # 팝업창 닫기
    try:
        driver.find_element(By.CLASS_NAME, "ab-close-button").click()
    except:
        pass
    search_company = driver.find_element(By.ID, "search_bar_search_query")
    search_company.send_keys(company)
    #엔터 누르는거
    search_company.send_keys(Keys.RETURN)
    driver.implicitly_wait(2)
    # 검색해서 나오는 회사중 맨 앞의 회사 클릭
    driver.find_element(By.CLASS_NAME, "tit").click()
    driver.implicitly_wait(2)

    # 팝업창 닫기 1   
    try:
        driver.find_element(By.CLASS_NAME,"ab-close-button").click()
        driver.implicitly_wait(4)
    except:
        pass
    
    #팝업창 닫기 2
    try:
        driver.find_element(By.CLASS_NAME, "btn_close_x_ty1").click()
        driver.implicitly_wait(4)
    except:
        pass

    # 연봉 탭 클릭
    selector = "#viewCompaniesMenu > ul > li.viewSalaries > a"
    link = driver.find_element(By.CSS_SELECTOR, selector)
    salary_url = link.get_attribute('href')
    driver.get(salary_url)
    driver.implicitly_wait(2)

    # 그 기업의 평균연봉 정보 수집
    try:
        average = driver.find_element(By.CLASS_NAME, "chart_header")
        average = average.find_element(By.CLASS_NAME, "num")
        average_salary = average.text
    except:
        average_salary = "정보 없음"

    return average_salary

# 워드클라우드 요약하는 함수
def wordcloud_info(company):
    s3_client = get_S3_connection()
    try:
        obj = s3_client.get_object(Bucket=Variable.get('AWS_S3_BUCKET'), Key=f"path/in/s3/bucket/review_infomation_{company}.csv")
        df = pd.read_csv(obj['Body'])
    except Exception as e:
        print(f"Failed to read CSV file from S3: {e}")
    words = []  

    # 명사 뽑는 함수 넣어주고
    hannanum = Hannanum()

    # 리뷰요약에 대해서, 많이 나오는 단어 새고, 한줄로 만듦
    for review in df['review_summary']:
        nouns = hannanum.nouns(review)
        words += nouns

    # words를 센다.
    review_summary_counters = Counter(words)
    # counter가 사전 형식임. 단어가 1자리보다는 많고, 2개이상 나오는 단어
    review_summary_counter = {k: v for k, v in review_summary_counters.items() if v >= 2 and len(k) > 1}

    # 위에꺼, 갯수가 많은 순서대로 정렬
    if len(review_summary_counter) > 0:
        key = sorted(review_summary_counter, key=review_summary_counter.get, reverse=True)
        review_summary_key = key[:10]
        # ,를 넣고 한줄로 만듦
        review_summary = ', '.join(review_summary_key)

     # 조건에 만족 못했어도 넣어야 하니까, 원본을 한줄로 만듦      
    else:
        key = sorted(review_summary_counters, key=review_summary_counters.get, reverse=True)
        review_summary_key = key[:10]
        review_summary = ', '.join(review_summary_key)

    # 리뷰의 장점에 대해서 위에와 똑같이 시행
    words = []
    for review in df['merit']:
        nouns = hannanum.nouns(review)
        words += nouns
    merit_counters = Counter(words)
    merit_counter = {k: v for k, v in merit_counters.items() if v >= 2 and len(k) > 1}
    if len(merit_counter) > 0:
        key = sorted(merit_counter, key=merit_counter.get, reverse=True)
        merit_key = key[:10]
        merit = ', '.join(merit_key)
    else:
        key = sorted(merit_counters, key=merit_counters.get, reverse=True)
        merit_key = key[:10]
        merit = ', '.join(merit_key)

    words = []
      # 리뷰의 단점에 대해서 위에와 똑같이 시행
    for review in df['demerit']:
        nouns = hannanum.nouns(review)
        words += nouns
    demerit_counters = Counter(words)
    demerit_counter = {k: v for k, v in demerit_counters.items() if v >= 2 and len(k) > 1}
    if len(demerit_counter) > 0:
        key = sorted(demerit_counter, key=demerit_counter.get, reverse=True)
        demerit_key = key[:10]
        demerit = ', '.join(demerit_key)
    else:
        key = sorted(demerit_counters, key=demerit_counters.get, reverse=True)
        demerit_key = key[:10]
        demerit = ', '.join(demerit_key)

    words = []
    # 인터뷰 요약에 대해서 위에와 똑같이 시행
    for review in df['interview_summary']:
        nouns = hannanum.nouns(review)
        words += nouns
    interview_summary_counters = Counter(words)
    interview_summary_counter = {k: v for k, v in interview_summary_counters.items() if v >= 2 and len(k) > 1}
    if len(interview_summary_counter) > 0:
        key = sorted(interview_summary_counter, key=interview_summary_counter.get, reverse=True)
        interview_summary_key = key[:10]
        interview_summary = ', '.join(interview_summary_key)
    else:
        key = sorted(interview_summary_counters, key=interview_summary_counters.get, reverse=True)
        interview_summary_key = key[:10]
        interview_summary = ', '.join(interview_summary_key)

    words = []
     # 인터뷰 질문에 대해서 위에와 똑같이 시행
    for review in df['interview_question']:
        nouns = hannanum.nouns(review)
        words += nouns
    interview_question_counters = Counter(words)
    interview_question_counter = {k: v for k, v in interview_question_counters.items() if v >= 2 and len(k) > 1}
    if len(interview_question_counter) > 0:
        key = sorted(interview_question_counter, key=interview_question_counter.get, reverse=True)
        interview_question_key = key[:10]
        interview_question = ', '.join(interview_question_key)
    else:
        key = sorted(interview_question_counters, key=interview_question_counters.get, reverse=True)
        interview_question_key = key[:10]
        interview_question = ', '.join(interview_question_key)

    words = []
    #면접 답변에 대해서 위와 같이 진행
    for review in df['interview_answer']:
        nouns = hannanum.nouns(review)
        words += nouns
    interview_answer_counters = Counter(words)
    interview_answer_counter = {k: v for k, v in interview_answer_counters.items() if v >= 2 and len(k) > 1}
    if len(interview_answer_counter) > 0:
        key = sorted(interview_answer_counter, key=interview_answer_counter.get, reverse=True)
        interview_answer_key = key[:10]
        interview_answer = ', '.join(interview_answer_key)
    else:
        key = sorted(interview_answer_counters, key=interview_answer_counters.get, reverse=True)
        interview_answer_key = key[:10]
        interview_answer = ', '.join(interview_answer_key)
     # 결과들 한줄로
    return [company, review_summary, merit, demerit, interview_summary, interview_question, interview_answer]



# 점핏에서 저장한 정보 가져와서, 회사 이름만 수집
def load_jumpit_file():
    #s3_hook = S3Hook(aws_conn_id='s3_conn')
    try:
        s3_client = get_S3_connection()
        bucket_name = Variable.get('AWS_S3_BUCKET')
        file_key = "jobinfo_jumpit.csv"
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        if 'company' not in df.columns:
            raise ValueError("'company' 컬럼이 데이터프레임에 존재하지 않습니다.")
        companies = df['company'].tolist()
        logging.info(companies)
        return companies
    except Exception as e:
        print(f"오류 발생: {e}")
        return []

def read_s3_file(bucket_name: str) -> None:
    conn = get_S3_connection()
    try:
        response = conn.list_objects(Bucket=bucket_name)
        file_list = [obj['Key'] for obj in response.get('Contents', [])]
        return file_list
    except Exception as e:
        print(f"Error getting file list from S3: {e}")
        return []


def upload_to_s3(dataframe, file_name, bucket_name):
    s3_clinet = get_S3_connection()
    try:
        csv_buffer = StringIO()
        dataframe.to_csv(csv_buffer, index=False)

        s3_clinet.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=file_name)
        print(f"File {file_name} uploaded to S3 successfully.")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")

default_args = {
    'start_date': datetime(2024,1,10),
}

with DAG(
    dag_id = "jobinfo_jobplanet",
    schedule_interval = '@once',
    default_args=default_args,
) as dag:
    
    task1 = PythonOperator(
        task_id = 'load_jumpit_file',
        python_callable=load_jumpit_file,
        provide_context = True,
        dag = dag,
    )
    task2 = PythonOperator(
        task_id = 'read_s3_file',
        python_callable=read_s3_file,
        provide_context = True,
        op_kwargs = {
            'bucket_name' : Variable.get('AWS_S3_BUCKET')
    },
        dag=dag
    )
    task3 = PythonOperator(
        task_id = 'create_infomation_s3',
        python_callable=create_infomation_s3,
        provide_context = True,
        dag=dag,
    )
    task4 = PythonOperator(
        task_id='create_summary_s3_to_rds',
        python_callable=create_summary_s3_to_rds,
        dag=dag,
    )

    task1 >> task2 >> task3 >> task4
