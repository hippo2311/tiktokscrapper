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
        top_threads=CommentsScraper.DEFAULT_TOP_THREADS,
        max_top_threads=CommentsScraper.MAX_TOP_THREADS,
    )


@app.post("/comments/export")
def export_comments_csv():
    video_url = (request.form.get("video_url") or "").strip()
    top_threads_raw = (
        request.form.get("top_threads") or str(CommentsScraper.DEFAULT_TOP_THREADS)
    ).strip()

    try:
        top_threads = max(1, min(int(top_threads_raw), CommentsScraper.MAX_TOP_THREADS))
    except ValueError:
        top_threads = CommentsScraper.DEFAULT_TOP_THREADS

    result = CommentsScraper(Settings()).scrape_top_comment_threads(
        video_url=video_url,
        limit=None,
        top_threads=top_threads,
    )
    if result.get("status") != "success":
        return jsonify(result), 400

    csv_content = build_comments_csv(result.get("comment_threads") or [])
    video_id = result.get("video_id") or "comments"
    file_name = f"tiktok-top-comment-threads-{video_id}.csv"

    return Response(
        csv_content,
        mimetype="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{file_name}"',
        },
    )


def build_comments_csv(comment_threads: list[dict]) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
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
        ]
    )

    for thread in comment_threads:
        thread_rank = thread.get("thread_rank") or ""
        main_comment = thread.get("main_comment") or {}
        root_comment_id = main_comment.get("comment_id") or ""

        rows: list[tuple[str, dict]] = [("main_comment", main_comment)]
        rows.extend(("subcomment", subcomment) for subcomment in (thread.get("subcomments") or []))

        for row_type, comment in rows:
            writer.writerow(
                [
                    row_type,
                    thread_rank,
                    root_comment_id if row_type == "subcomment" else (comment.get("comment_id") or ""),
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
