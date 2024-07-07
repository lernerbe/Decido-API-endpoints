# global imports
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import os
import pandas as pd
import io
from dotenv import load_dotenv
from src.utils.utils import create_chunks


load_dotenv()

pg_pool  = SimpleConnectionPool(minconn=2,
                                  maxconn=50,
                                  database=os.environ.get("DB_NAME"),
                                  user=os.environ.get("DB_USER"),
                                  password=os.environ.get("DB_PASS"),
                                  host=os.environ.get("DB_HOST"),
                                  port=os.environ.get("DB_PORT"))


class PostgresConnector:
    def __init__(self):
        self.conn_id = None

    def get_connection(self):

        if self.conn_id is not None:
            return self.conn_id

        self.conn_id = pg_pool.getconn()
        return self.conn_id

    def close_connection(self):
        if self.conn_id is not None:
            pg_pool.putconn(self.conn_id)
            self.conn_id = None

    def select_query(self, sql, binds=None):
        with self.get_connection().cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql, binds)
            result = pd.DataFrame(
                cursor.fetchall(), columns=cursor.column_mapping)
            return result

    def insert_query(self, schema, table, df: pd.DataFrame):
        columns = df.columns.tolist()
        sql = f"""
        COPY {schema}.{table} ({','.join(columns)}) 
        FROM STDIN 
        WITH CSV DELIMITER AS ','"""

        csv_buffer = io.StringIO(df.to_csv(index=False, header=False))

        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.copy_expert(sql, csv_buffer)
                conn.commit()
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error in insert_query: {e}") 
            

    def format_row(self, row):
        formatted_values = ', '.join([f"'{val}'"
                                    if isinstance(val, str) else str(val)
                                    if (val is not None and val is not pd.NA and not pd.isna(val)) else 'NULL'
                                    for val in row])
        return f"({formatted_values})"
    
    def upsert_query_without_deleting(self, schema, table, df: pd.DataFrame, unique_columns):
        for col in unique_columns:
            if col not in df.columns:
                raise ValueError(
                    f"Unique key column {col} not found in dataframe")

        # check if df is empty
        if df.empty:
            return

        values = ', '.join([self.format_row(row) for _, row in df.iterrows()])
        query = f'''
            INSERT INTO {schema}.{table} ({','.join(df.columns)}) 
            VALUES {values}
            ON CONFLICT ({','.join(unique_columns)}) DO UPDATE 
            SET ({','.join(df.columns)}) = ({','.join(['EXCLUDED.' + col for col in df.columns])});
        '''
        self.execute_query(query)

    def execute_query(self, sql, binds=None, commit=True):
        try:


            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(sql, binds)
                if commit:
                    conn.commit()
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error in execute_query: {e}") 

    def upsert_query(self, schema, table, df: pd.DataFrame, unique_columns):
        for col in unique_columns:
            if col not in df.columns:
                raise ValueError(
                    f"Unique key column {col} not found in dataframe")

        df_chunks = create_chunks(df, 5000)
        for df_chunk in df_chunks:
            delete_query = f"DELETE FROM {schema}.{table} WHERE ({','.join(unique_columns)}) IN ({','.join(['%s']*len(df_chunk))})"
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(delete_query, df_chunk[unique_columns[0]].tolist() if len(unique_columns) == 1 else list(
                    df_chunk[unique_columns].itertuples(index=False, name=None)))
            
            self.insert_query(schema, table, df_chunk)

    def update_query(self,schema,table,update_values:dict,update_condition:str):
        update_query = f"""
            UPDATE {schema}.{table} 
            SET {','.join([f'{key} = %s' for key in update_values.keys()])} 
            WHERE {update_condition}
            RETURNING *;
            """
        
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(update_query, tuple(update_values.values()))
                conn.commit()
                result = pd.DataFrame(
                    cursor.fetchall(), columns=cursor.column_mapping)
                return result
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error in update_query: {e}")

    def insert_query_regular(self,schema,table,coulmns:list,insert_values:list[list]):
        insert_query = f"""
            INSERT INTO {schema}.{table} ({','.join(coulmns)})
            VALUES  {','.join(['('+','.join([f"'{value}'" for value in row])+')' for row in insert_values])} 
            RETURNING *;
            """    
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(insert_query)
                conn.commit()
                result = pd.DataFrame(
                    cursor.fetchall(), columns=cursor.column_mapping)
                return result
        except Exception as e:
            conn.rollback()
            raise Exception(f"Error in insert_query_regular: {e}") 