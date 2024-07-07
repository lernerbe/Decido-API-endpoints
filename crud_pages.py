from src.models.BaseModels import PagesBody
from src.utils.postgres_connector import PostgresConnector
import pandas as pd

class User:

    @staticmethod
    def get_user_by_id(user_id:int):
        try:
            db_connector = PostgresConnector()
            result = db_connector.select_query("""          
                    SELECT
                        users.id,
                        users.first_name,
                        users.last_name,
                        teams.team_name,
                        users.email,
                        users.phone_number,
                        users.user_role,
                        users.user_status,
                        users.created_date
                    FROM issac_app.users users
                    LEFT JOIN issac_app.teams teams ON users.team_id = teams.id
                    WHERE 
                        1=1  
                    AND users.id=  %(user_id)s""",{"user_id":user_id})
            db_connector.close_connection()

            if result.empty:
                raise Exception(f"Error user_id {user_id} doesn't exist")

            #Normalize date to string
            result["created_date"] = result["created_date"].astype(str)
            return result.to_dict(orient='records')[0]

        except Exception as e:
            db_connector.close_connection()
            print(e)
            raise Exception(f"Error in services/users/get_user_by_id.py: \n {e}")
    