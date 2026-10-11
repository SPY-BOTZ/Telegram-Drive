
import math
import mimetypes
from urllib.parse import quote

from fastapi.responses import StreamingResponse, Response

from utils.logger import Logger
from utils.streamer.custom_dl import ByteStreamer
from utils.clients import get_client

logger = Logger(__name__)

class_cache = {}


async def media_streamer(
    channel: int,
    message_id: int,
    file_name: str,
    request,
):
    global class_cache

    faster_client = get_client()

    if faster_client in class_cache:
        tg_connect = class_cache[faster_client]
    else:
        tg_connect = ByteStreamer(faster_client)
        class_cache[faster_client] = tg_connect

    file_id = await tg_connect.get_file_properties(channel, message_id)
    file_size = file_id.file_size

    if file_size <= 0:
        return Response(
            content="File is empty",
            status_code=404,
        )

    range_header = request.headers.get("range")
    is_partial = bool(range_header)

    if range_header:
        try:
            unit, byte_range = range_header.split("=", 1)

            if unit.strip().lower() != "bytes" or "," in byte_range:
                raise ValueError("Unsupported range")

            start_text, end_text = byte_range.split("-", 1)

            if not start_text:
                suffix_length = int(end_text)

                if suffix_length <= 0:
                    raise ValueError("Invalid suffix range")

                from_bytes = max(file_size - suffix_length, 0)
                until_bytes = file_size - 1
            else:
                from_bytes = int(start_text)
                until_bytes = int(end_text) if end_text else file_size - 1

                if from_bytes >= file_size:
                    raise ValueError("Range starts beyond file")

                until_bytes = min(until_bytes, file_size - 1)

            if from_bytes < 0 or until_bytes < from_bytes:
                raise ValueError("Invalid byte range")

        except (ValueError, TypeError):
            return Response(
                status_code=416,
                content="Range Not Satisfiable",
                headers={
                    "Content-Range": f"bytes */{file_size}",
                    "Accept-Ranges": "bytes",
                },
            )
    else:
        from_bytes = 0
        until_bytes = file_size - 1

    chunk_size = 1024 * 1024

    offset = from_bytes - (from_bytes % chunk_size)
    first_part_cut = from_bytes - offset
    last_part_cut = (until_bytes % chunk_size) + 1

    req_length = until_bytes - from_bytes + 1

    # Include both the first and last Telegram file parts.
    part_count = (
        until_bytes // chunk_size
        - offset // chunk_size
        + 1
    )

    body = tg_connect.yield_file(
        file_id,
        offset,
        first_part_cut,
        last_part_cut,
        part_count,
        chunk_size,
    )

    mime_type = (
        mimetypes.guess_type(file_name)[0]
        or "application/octet-stream"
    )

    # Browser should play supported media instead of forcing a download.
    inline_types = (
        "video/",
        "audio/",
        "image/",
    )

    disposition = (
        "inline"
        if mime_type.startswith(inline_types)
        else "attachment"
    )

    encoded_name = quote(file_name, safe="")

    headers = {
        "Content-Type": mime_type,
        "Content-Length": str(req_length),
        "Content-Disposition": (
            f"{disposition}; filename*=UTF-8''{encoded_name}"
        ),
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-cache",
        "X-Content-Type-Options": "nosniff",
    }

    if is_partial:
        headers["Content-Range"] = (
            f"bytes {from_bytes}-{until_bytes}/{file_size}"
        )

    return StreamingResponse(
        body,
        status_code=206 if is_partial else 200,
        headers=headers,
        media_type=mime_type,
    )
    
