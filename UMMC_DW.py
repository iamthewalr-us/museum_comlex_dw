import pandas as pd
from datetime import datetime, date
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

month_dict = {
    'January': "Январь",
    'February': 'Февраль', 
    'March': 'Март', 
    'April': 'Апрель', 
    'May': 'Май', 
    'June': 'Июнь', 
    'July': 'Июль', 
    'August': 'Август', 
    'September': 'Сентябрь', 
    'October': 'Октябрь', 
    'November': 'Ноябрь', 
    'December': 'Декабрь'
}

week_day_dict = {
'Monday': 'Понедельник',
'Tuesday': 'Вторник',
'Wednesday': 'Среда',
'Thursday': 'Четверг',
'Friday': 'Пятница',
'Saturday': 'Суббота',
'Sunday': 'Воскресенье',
}


def extract_date_data():
    sql_stmt = "select datetime from mvt_transactions limit 1"
    pg_hook = PostgresHook(
        postgres_conn_id='raw_data_db',
        schema='raw_data'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchall()


def process_date_data(ti):
    date_xcom = ti.xcom_pull(task_ids=['extract_date_data'])
    if not date:
        raise Exception('No data.')
    
    date_df = pd.DataFrame(
        data=date_xcom[0],
        columns=["date_col"]
    )

    date_list = date_df.date_col[0].split(' ')[0].split('.')
    day = int(date_list[0])
    month = int(date_list[1])
    year = int(date_list[2])

    date_var = date(year=year, month=month, day=day)

    list_of_holidays = ['01.01', '07.01', '23.02', '08.03', '01.05', '09.05', '12.06', '04.11', '31.12']

    date_df = pd.DataFrame.from_dict({'date_key': [int(''.join(reversed(date_list)))], 
                                       'date_yyyymmdd': [date_var.isoformat()], 
                                       "day": [day], 
                                       "month": [month], 
                                       "year": [year],
                                        'day_of_the_week': [date_var.isoweekday()], 
                                        'name_of_the_day': [date_var.strftime('%A')], 
                                        'day_of_the_year': [date_var.strftime('%j')],
                                        'week_number': [date_var.strftime('%U')], 
                                        'month_name': [date_var.strftime('%B')], 
                                        'quarter': [((date_var.month-1)//3)+1], 
                                        'weekday': [date_var.weekday()>=5],
                                        'holiday': ['.'.join([str(day), str(month)]) in list_of_holidays], 
                                        'russian_month_name': [month_dict[date_var.strftime('%B')]], 
                                        'russian_day_name': [week_day_dict[date_var.strftime('%A')]]
                                        }
)

    date_df.to_csv(Variable.get('tmp_date_location'), index=False) 

def extract_raw_data():
    sql_stmt = "select * from mat_transactions mt \
                union \
                select * from mvt_transactions mt2 \
                union \
                select * from avia_transactions at2 \
                union \
                select * from cv1_transactions ct \
                union \
                select * from cv2_transactions ct2 "
    pg_hook = PostgresHook(
        postgres_conn_id='raw_data_db',
        schema='raw_data'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    df = pd.DataFrame(data=cursor.fetchall())
    df.to_csv(Variable.get('tmp_raw_data_location'), index=False)
    

def extract_transformed_data():
    sql_stmt = "select * from ticket_sale_fact "
    pg_hook = PostgresHook(
        postgres_conn_id='staging_db',
        schema='staging'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    df = pd.DataFrame(data=cursor.fetchall())
    df.to_csv(Variable.get('tmp_staging_location'), index=False)



with DAG(
    dag_id='ummc_dw_etl',
    schedule_interval='@daily',
    start_date=datetime(year=2023, month=1, day=1),
    catchup=False
) as dag:
    
    # 1. Fill up date table

    # 1.1. Extract data
    task_extract_date_data = PythonOperator(
        task_id='extract_date_data',
        python_callable=extract_date_data,
        do_xcom_push=True
    )

    # 1.2. Process the data
    task_process_date_data = PythonOperator(
        task_id='process_date_data',
        python_callable=process_date_data,
        do_xcom_push=True,
        op_kwargs={'month_dict': month_dict,
                   'week_day_dict': week_day_dict}
    )

    # 1.3. Save to DW database
    task_load_date_data_DW = BashOperator(
        task_id='load_date_data_DW',
        bash_command=(
            'psql -d UMMC_Museum_complex -U mironov1 -c "'
            'COPY date_dim(date_key, date_yyyymmdd, day, month, year, day_of_the_week, \
                name_of_the_day, day_of_the_year, week_number, month_name, quarter, \
                    is_weekend, holiday, russian_month_name, russian_day_name) '
            "FROM '/tmp/date_processed.csv' "
            "DELIMITER ',' "
            'CSV HEADER"'
        )
    )

    # 1.4 Save to staging database
    task_load_date_data_staging = BashOperator(
        task_id='load_date_data_staging',
        bash_command=(
            'psql -d staging -U mironov1 -c "'
            'COPY date_dim(date_key, date_yyyymmdd, day, month, year, day_of_the_week, \
                name_of_the_day, day_of_the_year, week_number, month_name, quarter, \
                    is_weekend, holiday, russian_month_name, russian_day_name) '
            "FROM '/tmp/date_processed.csv' "
            "DELIMITER ',' "
            'CSV HEADER"'
        )
    )

    #2. Extract data from source to the staging area

    #2.1 extract data from source
    task_extract_raw_data = PythonOperator(
        task_id='extract_raw_data',
        python_callable=extract_raw_data
    )

    # 2.2 load data to staging
    task_load_raw_data = BashOperator(
        task_id='load_raw_data',
        bash_command=(
            'psql -d staging -U mironov1 -c "'
            'COPY raw_data(datetime, code, pt_ord_num, "action", zonelevelname, clientcategory, \
                price, status_name, paymeanname, cashreg_num, cashier_name, quant, "cost") '
            "FROM '/Users/mironov1/airflow/tmp/raw_data.csv' "
            "DELIMITER ',' "
            'CSV HEADER"'
        )
    )

    # 3 Transform data
    task_transform_raw_data = BashOperator(
        task_id='transform_raw_data',
        bash_command=(
            'psql -d staging -U mironov1 -c "' \
            'insert into ticket_sale_fact '
            'select '
            'ds2.date_key, ds2.code, ds2.pt_ord_num, ds2.cashreg_num, ds2.product_key, '
            'ds2.price, ds2.quant, ds2."cost", ds2.status_key, ds2.time_key, ds2.category_key, ds2.payment_key '
            'from '
            '(select '
            '(ds.y || ds.m || ds.d) as date_key, ds.code, ds.pt_ord_num, ds.cashreg_num, ds.product_key, ds.price, '
            'cast(ds.quant as integer), cast(ds."cost" as integer), ds.status_key, cast(ds.time_key as integer), category_key, payment_key '
            'from '
            '(select '
            "split_part(split_part(datetime, ' ', 1), '.', 3) as y, "
            "split_part(split_part(datetime, ' ', 1), '.', 2) as m, "
            "split_part(split_part(datetime, ' ', 1), '.', 1) as d, "
            'code, pt_ord_num, cashreg_num, pd.product_key, price, quant, "cost", sd.status_key, '
            "split_part(split_part(datetime, ' ', 2), ':', 1) as time_key, "
            'category_key, payment_key '
            'from raw_data rd '
            'join product_dim pd '
            'on rd."action" = pd.russian_product_name '
            'join status_dim sd '
            'on rd.status_name = sd.status_name '
            'join visitor_category_dim vcd '
            'on rd.clientcategory = vcd.category_name '
            'join payment_method_dim pmd '
            'on rd.paymeanname = pmd.russian_payment_method_name)as ds) as ds2 '
            'join date_dim '
            'on ds2.date_key = date_dim.date_key "'
        )
    )

    # 4. Load data to DW

    # 4.1 Extracting data from staging
    task_extract_transformed_data = PythonOperator(
        task_id='extract_transformed_data',
        python_callable=extract_transformed_data
    )

    # 4.2 Loading to DW
    task_load_data_to_data_warehouse = BashOperator(
        task_id='load_data_to_data_warehouse',
        bash_command=(
            'psql -d UMMC_Museum_complex -U mironov1 -c "'
            'COPY ticket_sale_fact(date_key, receipt_number, ticket_number, cash_register_key,\
                product_key, price, quantitiy, total_revenue, status_key, time_key, visitors_category, payment_method) '
            "FROM '/Users/mironov1/airflow/tmp/staging_data.csv' "
            "DELIMITER ',' "
            'CSV HEADER"'
        )
    )

    # 5. Truncate tables in the staging area
    task_truncate_ticket_sale_fact_table = PostgresOperator(
	task_id='truncate_ticket_sale_fact_table',
	postgres_conn_id='staging_db',
	sql="TRUNCATE TABLE ticket_sale_fact"
)





    
    



    [task_extract_date_data, task_extract_raw_data] >> task_process_date_data >> \
        [task_load_date_data_DW, task_load_date_data_staging] >> task_load_raw_data >> task_transform_raw_data >> \
        task_extract_transformed_data >> task_load_data_to_data_warehouse >> task_truncate_ticket_sale_fact_table
    