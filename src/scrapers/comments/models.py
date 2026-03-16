from pydantic import BaseModel
from typing import Optional


class CommentData(BaseModel):
    comment_id: str
    author_username: Optional[str] = None
    author_display_name: Optional[str] = None
    text: str
    likes: int = 0
    replies: int = 0
    posted_at: Optional[str] = None
    is_reply: bool = False
    parent_comment_id: Optional[str] = None
