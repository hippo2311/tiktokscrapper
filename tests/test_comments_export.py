import csv
import io

import src.scrapers.comments.scraper as comments_scraper_module
from src.config.settings import Settings
from src.scrapers.comments.scraper import CommentsScraper
from src.webapp import build_comments_csv


def test_build_comments_csv_contains_top_comment_and_subcomments():
    csv_content = build_comments_csv(
        top_comment={
            "author_username": "top_user",
            "comment_id": "c1",
            "parent_comment_id": None,
            "text": "Top comment",
            "likes": 99,
            "posted_at": "2026-03-23T00:00:00+00:00",
        },
        subcomments=[
            {
                "author_username": "reply_user",
                "comment_id": "r1",
                "parent_comment_id": "c1",
                "text": "Reply",
                "likes": 7,
                "posted_at": "2026-03-23T00:01:00+00:00",
            }
        ],
    )

    rows = list(csv.reader(io.StringIO(csv_content.lstrip("\ufeff"))))

    assert rows == [
        [
            "row_type",
            "author_username",
            "comment_id",
            "parent_comment_id",
            "comment_text",
            "engagement_likes",
            "posted_at",
        ],
        [
            "top_comment",
            "top_user",
            "c1",
            "",
            "Top comment",
            "99",
            "2026-03-23T00:00:00+00:00",
        ],
        [
            "subcomment",
            "reply_user",
            "r1",
            "c1",
            "Reply",
            "7",
            "2026-03-23T00:01:00+00:00",
        ],
    ]


def test_scrape_top_comment_thread_fetches_replies_for_highest_liked_comment(monkeypatch):
    scraper = CommentsScraper(Settings())

    monkeypatch.setattr(comments_scraper_module, "validate_url", lambda value: value)
    monkeypatch.setattr(scraper, "_extract_video_id", lambda value: "123")
    monkeypatch.setattr(
        scraper,
        "_fetch_top_level_comments",
        lambda video_id, limit: [
            {
                "comment_id": "c1",
                "author_username": "first",
                "text": "first",
                "likes": 10,
                "replies": 1,
                "parent_comment_id": None,
            },
            {
                "comment_id": "c2",
                "author_username": "second",
                "text": "second",
                "likes": 42,
                "replies": 2,
                "parent_comment_id": None,
            },
        ],
    )

    calls: list[tuple[str, str, int]] = []

    def fake_fetch_replies(video_id: str, comment_id: str, remaining: int) -> list[dict]:
        calls.append((video_id, comment_id, remaining))
        return [
            {
                "comment_id": "r1",
                "author_username": "reply",
                "text": "reply",
                "likes": 5,
                "parent_comment_id": comment_id,
            }
        ]

    monkeypatch.setattr(scraper, "_fetch_replies", fake_fetch_replies)

    result = scraper.scrape_top_comment_thread("https://www.tiktok.com/@x/video/123", limit=20)

    assert result["status"] == "success"
    assert result["video_id"] == "123"
    assert result["top_comment"]["comment_id"] == "c2"
    assert result["subcomments"] == [
        {
            "comment_id": "r1",
            "author_username": "reply",
            "text": "reply",
            "likes": 5,
            "parent_comment_id": "c2",
        }
    ]
    assert calls == [("123", "c2", 2)]
    assert result["collected_count"] == 2
