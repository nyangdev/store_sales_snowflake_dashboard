import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

import uuid

def main():

    load_dotenv()

    db_connection_str = (
        f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        f"?charset={os.getenv('DB_CHARSET')}"
    )
    db_engine = create_engine(db_connection_str)

    # 외래키 제약 조건 피하기
    with db_engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
        conn.commit()
    print("외래 키 체크 비활성화")

    try:
        csv_path = './data/train.csv'
        df_source = pd.read_csv(csv_path)
        print("원본 CSV 파일 로드 성공")
    except FileNotFoundError:
        print(f"'{csv_path}' 경로에 파일이 없습니다.")
        df_source = None

    # dim_region
    print("dim_region 적재 시작....")
    try:
        # 데이터프레임이 비어있는가
        if df_source is not None and not df_source.empty:
            # 필요한 컬럼만 선택하고 중복 값 제거
            df_region = pd.DataFrame(df_source['Region'].unique(), columns=['region_name'])

            # 기본키 id 추가
            df_region.insert(0, 'region_id', df_region.index + 1)

            # 테이블에 적재
            df_region.to_sql('dim_region', con=db_engine, if_exists='append', index=False)         

            print("dim_region 적재 완료")

        else:
            print("원본 데이터가 비어있어 dim_region 적재를 건너뜁니다.")

    except Exception as e:
        print(f"dim_region 적재 중 오류 발생: {e}")


    # dim_city
    print("dim_city 적재 시작....")
    
    try:
        # 데이터프레임이 비어있는가
        if df_source is not None and not df_source.empty:
            # 필요한 컬럼 선택, 중복 값 제거
            df_city = df_source[['City', 'State', 'Region']].drop_duplicates().reset_index(drop=True)

            # region_id 가져오기
            df_city = pd.merge(df_city, df_region, left_on='Region', right_on='region_name', how='left')

            # 기본키 id 추가
            df_city.insert(0, 'city_id', df_city.index + 1)

            # 최종 컬럼 선택
            df_city_final = df_city[['city_id', 'City', 'State', 'region_id']].copy()

            # 최종 컬럼명 변경
            df_city_final.rename(columns={'City': 'city_name', 'State': 'state_name'}, inplace=True)

            # 테이블에 적재
            df_city_final.to_sql('dim_city', con=db_engine, if_exists='append', index=False)

            print("dim_city 적재 완료")

        else:
            print("원본 데이터가 비어있어 dim_city 적재를 건너뜁니다.")

    except Exception as e:
        print(f"dim_city 적재 중 오류 발생: {e}")

    # dim_segment
    print("dim_segment 적재 시작....")

    try:
        # 데이터프레임이 비어있는가
        if df_source is not None and not df_source.empty:
            # 필요한 컬럼 선택, 중복 값 제거
            df_segment = pd.DataFrame(df_source['Segment'].unique(), columns=['segment_name'])

            # 기본키 id 추가
            df_segment.insert(0, 'segment_id', df_segment.index + 1)

            # 테이블에 적재
            df_segment.to_sql('dim_segment', con=db_engine, if_exists='append', index=False)

            print("dim_segment 적재 완료")

        else:
            print("원본 데이터가 비어있어 dim_segment 적재를 건너뜁니다.")

    except Exception as e:
        print(f"dim_segment 적재 중 오류 발생: {e}")

    # dim_customer
    print("dim_customer 적재 시작....")
    
    try:
        # 데이터프레임이 비어있는가
        if df_source is not None and not df_source.empty:
            # 필요한 컬럼 선택, 중복 값 제거
            df_customer = df_source[['Customer Name', 'Segment']].drop_duplicates().reset_index(drop=True)

            # segment_id 가져오기
            df_customer = pd.merge(df_customer, df_segment, left_on='Segment', right_on='segment_name', how='left')

            # 기본키 id 추가
            df_customer.insert(0, 'customer_id', df_customer.index + 1)

            # 최종 컬럼 선택
            df_customer_final = df_customer[['customer_id', 'Customer Name', 'segment_id']].copy()

            # 최종 컬럼명 변경
            df_customer_final.rename(columns={'Customer Name': 'customer_name'}, inplace=True)

            # 테이블에 적재
            df_customer_final.to_sql('dim_customer', con=db_engine, if_exists='append', index=False)
            
            print("dim_customer 적재 완료")

        else:
            print("원본 데이터가 비어있어 dim_customer 적재를 건너뜁니다.")

    except Exception as e:
        print(f"dim_customer 적재 중 오류 발생: {e}")


    # dim_category
    print("dim_category 적재 시작....")

    try:
        # 데이터프레임이 비어있는가
        if df_source is not None and not df_source.empty:
            # 필요한 컬럼만 선택하고 중복 값 제거
            df_category = pd.DataFrame(df_source['Category'].unique(), columns=['category_name'])

            # 기본키 id 추가
            df_category.insert(0, 'category_id', df_category.index + 1)

            # 테이블에 적재
            df_category.to_sql('dim_category', con=db_engine, if_exists='append', index=False)

            print("dim_category 적재 완료")

        else:
            print("원본 데이터가 비어있어 dim_category 적재를 건너뜁니다.")

    except Exception as e:
        print(f"dim_category 적재 중 오류 발생: {e}")

    # dim_subcategory
    print("dim_subcategory 적재 시작....")

    try:
        # 데이터프레임이 비어있는가
        if df_source is not None and not df_source.empty:
            # 필요한 컬럼 선택, 중복 값 제거
            df_subcategory = df_source[['Sub-Category', 'Category']].drop_duplicates().reset_index(drop=True)

            # category_id 가져오기
            df_subcategory = pd.merge(df_subcategory, df_category, left_on='Category', right_on='category_name', how='left')

            # 기본키 id 추가
            df_subcategory.insert(0, 'subcategory_id', df_subcategory.index + 1)

            # 최종 컬럼 선택
            df_subcategory_final = df_subcategory[['subcategory_id', 'Sub-Category', 'category_id']].copy()

            # 최종 컬럼명 변경
            df_subcategory_final.rename(columns={'Sub-Category': 'subcategory_name', 'Category': 'category_id'}, inplace=True)

            # 테이블에 적재
            df_subcategory_final.to_sql('dim_subcategory', con=db_engine, if_exists='append', index=False)
            
            print("dim_subcategory 적재 완료")

        else:
            print("원본 데이터가 비어있어 dim_subcategory 적재를 건너뜁니다.")

    except Exception as e:
        print(f"dim_subcategory 적재 중 오류 발생: {e}")

    # dim_product
    print("dim_product 적재 시작....")
    
    try:
        # 데이터프레임이 비어있는가
        if df_source is not None and not df_source.empty:
            # 필요한 컬럼 선택, 중복 값 제거
            # product_name, category_id, subcategory_id
            df_product = df_source[['Product Name', 'Category', 'Sub-Category']].drop_duplicates().reset_index(drop=True)

            # category_id 가져오기
            df_product = pd.merge(df_product, df_category, left_on='Category', right_on='category_name', how='left')
            
            # subcategory_id 가져오기
            df_product = pd.merge(df_product, df_subcategory_final, left_on='Sub-Category', right_on='subcategory_name', how='left')

            # 기본키 id 추가
            df_product.insert(0, 'product_id', df_product.index + 1)

            # 최종 컬럼 선택
            df_product_final = df_product[['product_id','Product Name', 'category_id_x', 'subcategory_id']].copy()

            # 최종 컬럼명 변경
            df_product_final.rename(columns={'Product Name': 'product_name', 'category_id_x': 'category_id'}, inplace=True)

            # 테이블에 적재
            df_product_final.to_sql('dim_product', con=db_engine, if_exists='append', index=False)
            
            print("dim_product 적재 완료")

        else:
            print("원본 데이터가 비어있어 dim_product 적재를 건너뜁니다.")

    except Exception as e:
        print(f"dim_product 적재 중 오류 발생: {e}")


    # dim_orderdate
    print("dim_orderdate 적재 시작....")

    try:
        # 데이터프레임이 비어있는가
        if df_source is not None and not df_source.empty:
            # 필요한 컬럼만 선택하고 중복 값 제거
            df_orderdate = pd.DataFrame(df_source['Order Date'].unique(), columns=['orderdate_when'])

            df_orderdate['orderdate_when'] = pd.to_datetime(df_orderdate['orderdate_when'], dayfirst=True)

            # 기본키 id 추가
            df_orderdate.insert(0, 'orderdate_id', df_orderdate.index + 1)

            # 테이블에 적재
            df_orderdate.to_sql('dim_orderdate', con=db_engine, if_exists='append', index=False)         

            print("dim_orderdate 적재 완료")

        else:
            print("원본 데이터가 비어있어 dim_orderdate 적재를 건너뜁니다.")

    except Exception as e:
        print(f"dim_orderdate 적재 중 오류 발생: {e}")


    # fact_order
    print("fact_order 적재 시작....")

    try:
        # 데이터프레임이 비어있는가
        if df_source is not None and not df_source.empty:
            # 필요한 컬럼 선택, 중복 값 제거
            # df_fact = df_source[['Order ID', 'Sales', 'City', 'Customer Name', 'Product Name', 'Order Date']].drop_duplicates().reset_index(drop=True)

            df_fact = df_source.groupby(['Order ID', 'Product Name']).agg(
                Sales=('Sales', 'sum'),
                City=('City', 'first'),
                Customer_Name=('Customer Name', 'first'),
                Order_Date=('Order Date', 'first')
            ).reset_index()

            df_fact.rename(columns={
                'Customer_Name': 'Customer Name',
                'Order_Date': 'Order Date'
                }, inplace=True)

            # Order ID 컬럼에 비어있는 값이 있으면 기본값 넣어줌
            missing_orders = df_fact['Order ID'].isnull().sum()
            
            if missing_orders > 0:
                print(f"{missing_orders} 개의 행에서 Order ID가 비어있습니다.")

                df_fact['Order ID'] = df_fact['Order ID'].apply(
                    lambda x: f"UNKNOWN-{uuid.uuid4()}" if pd.isnull(x) else x
                )

            # city_id, customer_id, product_id, orderdate_id 가져오기

            # 1. city_id
            df_fact = pd.merge(df_fact, df_city_final, left_on='City', right_on='city_name', how='left')

            # 2. customer_id
            df_fact = pd.merge(df_fact, df_customer_final, left_on='Customer Name', right_on='customer_name', how='left')

            # 3. product_id
            df_fact = pd.merge(df_fact, df_product_final, left_on= 'Product Name', right_on='product_name', how='left')

            # 4. orderdate_id
            df_fact['Order Date'] = pd.to_datetime(df_fact['Order Date'], dayfirst=True).dt.date
            df_orderdate['orderdate_when'] = pd.to_datetime(df_orderdate['orderdate_when'], dayfirst=True).dt.date
            df_fact = pd.merge(df_fact, df_orderdate, left_on="Order Date", right_on="orderdate_when", how="left")

            # 최종 컬럼 선택
            df_fact_final = df_fact[['Order ID', 'Sales', 'city_id', 'customer_id', 'product_id', 'orderdate_id']].copy()

            # 최종 컬럼명 변경
            df_fact_final.rename(columns={'Order ID': 'order_id', 'Sales': 'sales'}, inplace=True)

            # 테이블에 적재
            df_fact_final.to_sql('fact_order', con=db_engine, if_exists='append', index=False)

            print("fact_order 적재 완료")

        else:
            print("원본 데이터가 비어있어 fact_order 적재를 건너뜁니다.")

    except Exception as e:
        print(f"fact_order 적재 중 오류 발생: {e}")

    # 외래키 제약 조건 피하기
    with db_engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))
        conn.commit()
    print("외래 키 체크 다시 활성화")

if __name__ == '__main__' :
    main()