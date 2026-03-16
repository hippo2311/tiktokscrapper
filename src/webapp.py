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
        limit = max(1, min(int(limit_raw), 200))
    except ValueError:
        limit = 20

    result = CommentsScraper(Settings()).scrape(video_url=video_url, limit=limit)
    if result.get("status") != "success":
        return jsonify(result), 400

    csv_content = build_comments_csv(result.get("comments") or [])
    video_id = result.get("video_id") or "comments"
    file_name = f"tiktok-comments-{video_id}.csv"

    return Response(
        csv_content,
        mimetype="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{file_name}"',
        },
    )


def build_comments_csv(comments: list[dict]) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["author_username", "comment_id", "comment_text", "posted_at"])

    for comment in comments:
        writer.writerow(
            [
                comment.get("author_username") or "",
                comment.get("comment_id") or "",
                comment.get("text") or "",
                comment.get("posted_at") or "",
            ]
        )

    return "\ufeff" + output.getvalue()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
