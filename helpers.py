from typing import Optional
from src.utils.utils import create_search_query
from src.utils.postgres_connector import PostgresConnector
from src.models.BaseModels import PageSearchFilter, AccountSearchFilter, AccountBody
import pandas as pd
from fastapi import HTTPException, status


###################/ PAGES HELPERS \###################
def get_pages_by_search(page: int, limit: int, search: Optional[str] = None, filters: Optional[PageSearchFilter] = None) -> dict[str, any]:
    try:
        bind_object = {"limit": limit, "offset": (page - 1) * limit}
        
        main_query = """
            SELECT 
            page_id,
            page_name,
            current_ad_limit,
            max_ad_limit,
            page_status,      
            last_fetch_time
            FROM facebook_search.fb_pages
            WHERE 1=1
        """ 

        # filter by page status
        if filters:
            if filters.page_status:
                main_query += " AND LOWER(page_status) = %(page_status)s"
                bind_object["page_status"] = filters.page_status
      
        # Add search functionality
        if search is not None:
            search_columns_list = ["page_id", "page_name"]
            main_query, bind_object = create_search_query(search, search_columns_list, main_query, bind_object)

        db_connector = PostgresConnector()

        # Get the total number of pages in the database
        total_pages_query = "SELECT COUNT(*) as total_count FROM facebook_search.fb_pages"
        total_pages_result = db_connector.select_query(total_pages_query)
        total_pages = total_pages_result["total_count"][0]

        # Get the actual page data
        result = db_connector.select_query(f"{main_query} LIMIT %(limit)s OFFSET %(offset)s", bind_object)
        db_connector.close_connection()

        pages_response = result.to_dict(orient='records') if not result.empty else []

        return {
            "total_pages": total_pages,
            "pages": int(total_pages / limit) + (1 if (total_pages % limit > 0) else 0),
            "pages_response": pages_response
        }

    except Exception as e:
        db_connector.close_connection()
        print(e)
        raise Exception(f"Error in get_pages_by_search: {e}")   
    


def edit_page_status(page_id: str, page_body: PageSearchFilter) -> dict[str, any]:
    try:
        db_connector = PostgresConnector()
        # Extract the page status from the request body
        new_status = page_body.page_status
        
        # Construct the SQL update query
        update_page_body = db_connector.update_query(
            "facebook_search",
            "fb_pages",
            {"page_status": new_status},
            f"page_id = CAST({page_id} AS TEXT)"
        )

        db_connector.close_connection()
        return {"detail": "Page status updated successfully"}

    except Exception as e:
        db_connector.close_connection()
        raise Exception(f"Error updating page status: {e}")
    
###################/ ACCOUNT HELPERS \###################


def get_accounts_by_search_and_filter(page: int, limit: int, search: Optional[str] = None, filters: Optional[AccountSearchFilter] = None) -> dict[str, any]:
    try:
        bind_object = {"limit": limit, "offset": (page - 1) * limit}

        main_query = """
            SELECT
            account_id,
            account_name,
            account_status,
            timezone_name,
            disable_reason,
            buyer_id,
            feed_provider_id,
            last_fetch_time
            FROM facebook_search.fb_ad_acoounts
            WHERE 1=1
        """
        # filter by page status
        if filters:
            if filters.account_status:
                main_query += " AND LOWER(account_status) = %(account_status)s"
                bind_object["account_status"] = filters.account_status
            if filters.timezone_name and filters.timezone_name != 'string':
                main_query += " AND LOWER(timezone_name) = LOWER(%(timezone_name)s)"
                bind_object["timezone_name"] = filters.timezone_name
            if filters.buyer_id and filters.buyer_id != 0:
                main_query += " AND buyer_id = %(buyer_id)s"
                bind_object["buyer_id"] = filters.buyer_id
            if filters.feed_provider_id and filters.feed_provider_id != 0:
                main_query += " AND feed_provider_id = %(feed_provider_id)s"
                bind_object["feed_provider_id"] = filters.feed_provider_id

        if search is not None:
            search_columns_list = ["account_id", "account_name"]
            main_query, bind_object = create_search_query(search, search_columns_list, main_query, bind_object)

        db_connector = PostgresConnector()

        # Get the total number of pages in the database
        total_accounts_query = "SELECT COUNT(*) as total_count FROM facebook_search.fb_ad_acoounts"
        total_accounts_result = db_connector.select_query(total_accounts_query)
        total_accounts = total_accounts_result["total_count"][0]

        result = db_connector.select_query(f"{main_query} LIMIT %(limit)s OFFSET %(offset)s", bind_object)
        db_connector.close_connection()

        accounts_response = result.to_dict(orient='records') if not result.empty else []

        return {
            "total_accounts": total_accounts,
            "accounts": int(total_accounts / limit) + (1 if (total_accounts % limit > 0) else 0),
            "accounts_response": accounts_response
        }

    except Exception as e:
        db_connector.close_connection()
        print(e)
        raise Exception(f"Error in get_accounts_by_search: {e}")   

def edit_account(account_id: str, account_body: AccountBody) -> dict[str, any]:
    try:
        db_connector = PostgresConnector()
        accounts_dic = account_body.model_dump(mode="unchanged")
        print(accounts_dic)

        # Extract the page status from the request bod

        
        # Construct the SQL update query
        update_account_body = db_connector.update_query(
            "facebook_search",
            "fb_ad_acoounts",
            accounts_dic,
            f"account_id = '{account_id}'"
        )
        db_connector.close_connection()
        return {"detail": "Account status updated successfully"}

    except Exception as e:
        db_connector.close_connection()
        raise Exception(f"Error updating account status: {e}")