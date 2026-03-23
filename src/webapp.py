from __future__ import annotations

import csv
import io

from flask import Flask, Response, jsonify, render_template, request

from src.config.settings import Settings
from src.scrapers.comments.scraper import CommentsScraper


app = Flask(__name__, template_folder="../templates", static_folder="../static")


@app.get("/")
def index():
    return render_template(
        "index.html",
        video_url="",
        limit=20,
    )


@app.post("/comments/export")
def export_comments_csv():
    video_url = (request.form.get("video_url") or "").strip()
    limit_raw = (request.form.get("limit") or "20").strip()

    try:
        limit = max(1, min(int(limit_raw), 500))
    except ValueError:
        limit = 20

    result = CommentsScraper(Settings()).scrape_top_comment_thread(video_url=video_url, limit=limit)
    if result.get("status") != "success":
        return jsonify(result), 400

    csv_content = build_comments_csv(
        top_comment=result.get("top_comment"),
        subcomments=result.get("subcomments") or [],
    )
    video_id = result.get("video_id") or "comments"
    file_name = f"tiktok-top-comment-thread-{video_id}.csv"

    return Response(
        csv_content,
        mimetype="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{file_name}"',
        },
    )


def build_comments_csv(top_comment: dict | None, subcomments: list[dict]) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "row_type",
            "author_username",
            "comment_id",
            "parent_comment_id",
            "comment_text",
            "engagement_likes",
            "posted_at",
        ]
    )

    rows: list[tuple[str, dict]] = []
    if top_comment:
        rows.append(("top_comment", top_comment))
    rows.extend(("subcomment", subcomment) for subcomment in subcomments)

    for row_type, comment in rows:
        writer.writerow(
            [
                row_type,
                comment.get("author_username") or "",
                comment.get("comment_id") or "",
                comment.get("parent_comment_id") or "",
                comment.get("text") or "",
                comment.get("likes") or 0,
                comment.get("posted_at") or "",
            ]
        )

    return "\ufeff" + output.getvalue()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
