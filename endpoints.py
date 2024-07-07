from fastapi import APIRouter,Body,HTTPException,Response,status,Depends,Query,Path
import pandas as pd
from typing import Annotated
from fastapi import APIRouter, Query, HTTPException, status
from typing import List, Optional
from src.helpers import get_pages_by_search, get_accounts_by_search_and_filter, edit_page_status, edit_account
from src.models.BaseModels import PageSearchFilter, AccountSearchFilter, AccountBody

###################/ PAGES ENDPOINTS \###################
router = APIRouter(tags=["status"])

@router.post("/pages")
def get_pages(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=30, ge=5, le=50),
    search: Optional[str] = None,
    filters: Optional[PageSearchFilter] = None
):
    try:
        pages = pd.DataFrame(get_pages_by_search(page, limit, search, filters))
        return pages.to_dict(orient='records')
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving pages: {str(e)}"
        )

@router.put("/pages/{page_id}")
def update_page_status(page_id: str, page_update_body: PageSearchFilter):
    try:

        result = edit_page_status(page_id, page_update_body)
        return result
    
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating page status: {str(e)}"
        )
    
###################/ ACCOUNT ENDPOINTS \###################

@router.post("/accounts")
def get_accounts(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=30, ge=5, le=50),
    search: Optional[str] = None,
    filters: Optional[AccountSearchFilter] = None
):
    try:
        accounts = pd.DataFrame(get_accounts_by_search_and_filter(page, limit, search, filters))
        return accounts.to_dict(orient='records')

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving accounts: {str(e)}"
        )
    

@router.put("/accounts/{account_id}")
def update_page_status(account_id: str, account_update_body: AccountBody):
    try:

        result = edit_account(account_id, account_update_body)
        return result

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error updating account: {str(e)}"
        )