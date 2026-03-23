from __future__ import annotations

import re
from datetime import datetime, timezone
from urllib.parse import urlparse

from src.scrapers.base import BaseScraper
from src.utils.validation import validate_url
from src.utils.logger import get_logger
from src.scrapers.comments.models import CommentData

log = get_logger(__name__)


class CommentsScraper(BaseScraper):
    API_ROOT = "https://www.tiktok.com/api/comment"
    VIDEO_ID_RE = re.compile(r"/video/(\d+)")
    MOBILE_VIDEO_ID_RE = re.compile(r"/v/(\d+)(?:\.html)?")
    FALLBACK_ID_RE = re.compile(r"(?<!\d)(\d{15,25})(?!\d)")

    def scrape(self, video_url: str, limit: int = 20) -> dict:
        try:
            video_url = validate_url(video_url)
            limit = max(1, min(int(limit), 500))

            video_id = self._extract_video_id(video_url)
            comments = self._fetch_comments(video_id=video_id, limit=limit)

            return self.ok(
                {
                    "video_url": video_url,
                    "video_id": video_id,
                    "comments": comments,
                    "collected_count": len(comments),
                }
            )
        except Exception as e:
            log.exception("Comments scraping failed")
            return self.fail(str(e), data={"video_url": video_url, "comments": []})

    def scrape_top_comment_thread(self, video_url: str, limit: int = 20) -> dict:
        try:
            video_url = validate_url(video_url)
            limit = max(1, min(int(limit), 500))

            video_id = self._extract_video_id(video_url)
            top_level_comments = self._fetch_top_level_comments(video_id=video_id, limit=limit)
            top_comment = self._select_top_comment(top_level_comments)

            subcomments: list[dict] = []
            if top_comment:
                reply_total = int(top_comment.get("replies") or 0)
                if reply_total:
                    subcomments = self._fetch_replies(
                        video_id=video_id,
                        comment_id=str(top_comment.get("comment_id") or ""),
                        remaining=reply_total,
                    )

            return self.ok(
                {
                    "video_url": video_url,
                    "video_id": video_id,
                    "top_comment": top_comment,
                    "subcomments": subcomments,
                    "scanned_comments": len(top_level_comments),
                    "collected_count": (1 if top_comment else 0) + len(subcomments),
                }
            )
        except Exception as e:
            log.exception("Top comment thread scraping failed")
            return self.fail(
                str(e),
                data={"video_url": video_url, "top_comment": None, "subcomments": []},
            )

    def _extract_video_id(self, video_url: str) -> str:
        candidates = [video_url]
        resolved = self._resolve_redirect_url(video_url)
        if resolved and resolved != video_url:
            candidates.append(resolved)

        for candidate in candidates:
            match = self.VIDEO_ID_RE.search(candidate)
            if match:
                return match.group(1)

            match = self.MOBILE_VIDEO_ID_RE.search(candidate)
            if match:
                return match.group(1)

            parsed = urlparse(candidate)
            segments = [segment for segment in parsed.path.split("/") if segment]
            for segment in reversed(segments):
                if segment.isdigit() and len(segment) >= 15:
                    return segment

            match = self.FALLBACK_ID_RE.search(candidate)
            if match:
                return match.group(1)

        raise ValueError("Could not extract TikTok video id from URL.")

    def _resolve_redirect_url(self, video_url: str) -> str | None:
        try:
            response = self.http.session.get(
                video_url,
                timeout=self.settings.timeout_s,
                proxies=self.settings.proxies,
                allow_redirects=True,
            )
            response.raise_for_status()
            return response.url
        except Exception:
            return None

    def _fetch_comments(self, video_id: str, limit: int) -> list[dict]:
        top_level_comments = self._fetch_top_level_comments(video_id=video_id, limit=limit)
        collected: list[dict] = []

        for comment in top_level_comments:
            if len(collected) >= limit:
                break

            collected.append(comment)

            reply_total = int(comment.get("replies") or 0)
            if reply_total and len(collected) < limit:
                replies = self._fetch_replies(
                    video_id=video_id,
                    comment_id=str(comment.get("comment_id") or ""),
                    remaining=limit - len(collected),
                )
                collected.extend(replies)

        return collected[:limit]

    def _fetch_top_level_comments(self, video_id: str, limit: int) -> list[dict]:
        collected: list[dict] = []
        cursor = 0

        while len(collected) < limit:
            payload = self.http.get_json(
                f"{self.API_ROOT}/list/?aid=1988&aweme_id={video_id}&count=20&cursor={cursor}"
            )
            self._raise_for_status(payload, "comments")

            items = payload.get("comments") or []
            if not items:
                break

            for item in items:
                if len(collected) >= limit:
                    break
                collected.append(self._map_comment(item))

            if not payload.get("has_more"):
                break

            cursor = int(payload.get("cursor") or 0)

        return collected[:limit]

    def _fetch_replies(self, video_id: str, comment_id: str, remaining: int) -> list[dict]:
        replies: list[dict] = []
        cursor = 0

        while len(replies) < remaining:
            payload = self.http.get_json(
                f"{self.API_ROOT}/list/reply/?aid=1988&aweme_id={video_id}&comment_id={comment_id}&count=20&cursor={cursor}"
            )
            self._raise_for_status(payload, "replies")

            items = payload.get("comments") or []
            if not items:
                break

            for item in items:
                if len(replies) >= remaining:
                    break
                replies.append(self._map_comment(item, is_reply=True, parent_comment_id=comment_id))

            if not payload.get("has_more"):
                break

            cursor = int(payload.get("cursor") or 0)

        return replies

    def _raise_for_status(self, payload: dict, label: str) -> None:
        status_code = int(payload.get("status_code") or 0)
        if status_code != 0:
            raise RuntimeError(
                f"TikTok {label} API returned status_code={status_code}: {payload.get('status_msg', '')}"
            )

    def _select_top_comment(self, comments: list[dict]) -> dict | None:
        if not comments:
            return None
        return max(
            comments,
            key=lambda comment: (
                int(comment.get("likes") or 0),
                int(comment.get("replies") or 0),
                str(comment.get("comment_id") or ""),
            ),
        )

    def _map_comment(
        self, item: dict, *, is_reply: bool = False, parent_comment_id: str | None = None
    ) -> dict:
        user = item.get("user") or {}
        return CommentData(
            comment_id=str(item.get("cid") or ""),
            author_username=user.get("unique_id"),
            author_display_name=user.get("nickname"),
            text=str(item.get("text") or ""),
            likes=int(item.get("digg_count") or 0),
            replies=int(item.get("reply_comment_total") or 0),
            posted_at=self._to_iso(item.get("create_time")),
            is_reply=is_reply,
            parent_comment_id=parent_comment_id,
        ).model_dump()

    def _to_iso(self, value: int | str | None) -> str | None:
        if value in (None, ""):
            return None
        timestamp = int(value)
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
