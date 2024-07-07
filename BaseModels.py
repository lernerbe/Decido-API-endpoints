from pydantic import BaseModel, Field
from typing import Optional, Literal, Annotated

# class PagesBody(BaseModel):
#     page_id: Optional[int] = None
#     page_name: Optional[str] = None
#     current_ad_limit: Optional[int] = None
#     max_ad_limit: Optional[int] = None
#     page_status: Literal['ACTIVE', 'INACTIVE', 'CLOSED'] = 'ACTIVE'
#     last_fetch_time: Optional[str] = None


class PageSearchFilter(BaseModel):
    page_status: Literal['active', 'inactive', 'closed'] = 'active'

# accounts
class AccountBody(BaseModel):
    account_status: Literal['active', 'inactive', 'closed'] = 'active'
    timezone_name: str
    buyer_id: int
    feed_provider_id: int

class AccountSearchFilter(BaseModel):
    account_status: Literal['active', 'inactive', 'closed'] = 'active'
    timezone_name: Optional[str] = None
    buyer_id: Optional[int] = None
    feed_provider_id: Optional[int] = None
