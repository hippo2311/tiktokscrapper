import csv
import io

import src.scrapers.comments.scraper as comments_scraper_module
from src.config.settings import Settings
from src.scrapers.comments.scraper import CommentsScraper
from src.webapp import app, build_comments_csv


def test_build_comments_csv_contains_main_comments_and_subcomments():
    csv_content = build_comments_csv(
        [
            {
                "thread_rank": 1,
                "main_comment": {
                    "author_username": "top_user",
                    "comment_id": "c1",
                    "parent_comment_id": None,
                    "text": "Top comment",
                    "likes": 99,
                    "posted_at": "2026-03-23T00:00:00+00:00",
                },
                "subcomments": [
                    {
                        "author_username": "reply_user",
                        "comment_id": "r1",
                        "parent_comment_id": "c1",
                        "text": "Reply",
                        "likes": 7,
                        "posted_at": "2026-03-23T00:01:00+00:00",
                    }
                ],
            },
            {
                "thread_rank": 2,
                "main_comment": {
                    "author_username": "second_user",
                    "comment_id": "c2",
                    "parent_comment_id": None,
                    "text": "Second top comment",
                    "likes": 50,
                    "posted_at": "2026-03-23T00:02:00+00:00",
                },
                "subcomments": [],
            }
        ]
    )

    rows = list(csv.reader(io.StringIO(csv_content.lstrip("\ufeff"))))

    assert rows == [
        [
            "row_type",
            "thread_rank",
            "root_comment_id",
            "author_username",
            "comment_id",
            "parent_comment_id",
            "comment_text",
            "engagement_likes",
            "posted_at",
        ],
        [
            "main_comment",
            "1",
            "c1",
            "top_user",
            "c1",
            "",
            "Top comment",
            "99",
            "2026-03-23T00:00:00+00:00",
        ],
        [
            "subcomment",
            "1",
            "c1",
            "reply_user",
            "r1",
            "c1",
            "Reply",
            "7",
            "2026-03-23T00:01:00+00:00",
        ],
        [
            "main_comment",
            "2",
            "c2",
            "second_user",
            "c2",
            "",
            "Second top comment",
            "50",
            "2026-03-23T00:02:00+00:00",
        ],
    ]


def test_scrape_top_comment_threads_fetches_replies_for_highest_liked_comments(monkeypatch):
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
            {
                "comment_id": "c3",
                "author_username": "third",
                "text": "third",
                "likes": 42,
                "replies": 1,
                "parent_comment_id": None,
            },
        ],
    )

    calls: list[tuple[str, str, int]] = []

    def fake_fetch_replies(video_id: str, comment_id: str, remaining: int) -> list[dict]:
        calls.append((video_id, comment_id, remaining))
        return [{
            "comment_id": f"r-{comment_id}",
            "author_username": "reply",
            "text": "reply",
            "likes": 5,
            "parent_comment_id": comment_id,
        }]

    monkeypatch.setattr(scraper, "_fetch_replies", fake_fetch_replies)

    result = scraper.scrape_top_comment_threads(
        "https://www.tiktok.com/@x/video/123",
        limit=20,
        top_threads=2,
    )

    assert result["status"] == "success"
    assert result["video_id"] == "123"
    assert result["selected_main_comments"] == 2
    assert result["comment_threads"] == [
        {
            "thread_rank": 1,
            "main_comment": {
                "comment_id": "c2",
                "author_username": "second",
                "text": "second",
                "likes": 42,
                "replies": 2,
                "parent_comment_id": None,
            },
            "subcomments": [
                {
                    "comment_id": "r-c2",
                    "author_username": "reply",
                    "text": "reply",
                    "likes": 5,
                    "parent_comment_id": "c2",
                }
            ],
        },
        {
            "thread_rank": 2,
            "main_comment": {
                "comment_id": "c3",
                "author_username": "third",
                "text": "third",
                "likes": 42,
                "replies": 1,
                "parent_comment_id": None,
            },
            "subcomments": [
                {
                    "comment_id": "r-c3",
                    "author_username": "reply",
                    "text": "reply",
                    "likes": 5,
                    "parent_comment_id": "c3",
                }
            ],
        },
    ]
    assert calls == [("123", "c2", 2), ("123", "c3", 1)]
    assert result["collected_count"] == 4


def test_fetch_top_level_comments_scans_all_pages_when_limit_is_none(monkeypatch):
    scraper = CommentsScraper(Settings())
    payloads = iter(
        [
            {
                "status_code": 0,
                "comments": [{"cid": "c1"}, {"cid": "c2"}],
                "has_more": True,
                "cursor": 20,
            },
            {
                "status_code": 0,
                "comments": [{"cid": "c3"}],
                "has_more": False,
                "cursor": 40,
            },
        ]
    )

    monkeypatch.setattr(scraper.http, "get_json", lambda url: next(payloads))
    monkeypatch.setattr(
        scraper,
        "_map_comment",
        lambda item, **kwargs: {"comment_id": item["cid"]},
    )

    comments = scraper._fetch_top_level_comments(video_id="123", limit=None)

    assert comments == [
        {"comment_id": "c1"},
        {"comment_id": "c2"},
        {"comment_id": "c3"},
    ]


def test_export_comments_csv_passes_top_threads_to_scraper(monkeypatch):
    calls: dict[str, object] = {}

    def fake_scrape_top_comment_threads(
        self, video_url: str, limit: int | None, top_threads: int
    ) -> dict:
        calls.update(
            {
                "video_url": video_url,
                "limit": limit,
                "top_threads": top_threads,
            }
        )
        return {
            "status": "success",
            "video_id": "123",
            "comment_threads": [],
        }

    monkeypatch.setattr(
        CommentsScraper,
        "scrape_top_comment_threads",
        fake_scrape_top_comment_threads,
    )

    response = app.test_client().post(
        "/comments/export",
        data={
            "video_url": "https://www.tiktok.com/@x/video/123",
            "top_threads": "3",
        },
    )

    assert response.status_code == 200
    assert response.headers["Content-Disposition"] == (
        'attachment; filename="tiktok-top-comment-threads-123.csv"'
    )
    assert calls == {
        "video_url": "https://www.tiktok.com/@x/video/123",
        "limit": None,
        "top_threads": 3,
    }
