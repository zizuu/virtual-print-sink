#!/usr/bin/env python3
import argparse
import struct
import urllib.request
from pathlib import Path


def attr(tag: int, name: str, value: bytes) -> bytes:
    name_b = name.encode("utf-8")
    return bytes([tag]) + struct.pack(">H", len(name_b)) + name_b + struct.pack(">H", len(value)) + value


def build_print_job(data: bytes, uri: str, job_name: str, document_format: str) -> bytes:
    # IPP/2.0, Print-Job (0x0002), request-id=1
    body = bytearray(b"\x02\x00\x00\x02\x00\x00\x00\x01")
    body.append(0x01)  # operation-attributes-tag
    body += attr(0x47, "attributes-charset", b"utf-8")
    body += attr(0x48, "attributes-natural-language", b"en")
    body += attr(0x45, "printer-uri", uri.encode("utf-8"))
    body += attr(0x42, "requesting-user-name", b"test-user")
    body += attr(0x42, "job-name", job_name.encode("utf-8"))
    body += attr(0x49, "document-format", document_format.encode("ascii"))
    body.append(0x03)  # end-of-attributes-tag
    body += data
    return bytes(body)


def main() -> None:
    parser = argparse.ArgumentParser(description="Send a simple IPP Print-Job to Virtual Print Sink")
    parser.add_argument("file", type=Path, help="File to send")
    parser.add_argument("--port", type=int, default=8631)
    parser.add_argument("--format", default="application/octet-stream", dest="document_format")
    args = parser.parse_args()

    data = args.file.read_bytes()
    ipp_uri = f"ipp://127.0.0.1:{args.port}/printers/virtual"
    http_url = f"http://127.0.0.1:{args.port}/printers/virtual"
    payload = build_print_job(data, ipp_uri, args.file.name, args.document_format)

    req = urllib.request.Request(
        http_url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/ipp"},
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        reply = response.read()
        if len(reply) < 8:
            raise SystemExit(f"Short IPP response: {reply!r}")
        ipp_status = struct.unpack(">H", reply[2:4])[0]
        request_id = struct.unpack(">I", reply[4:8])[0]
        print(f"HTTP {response.status}, IPP status=0x{ipp_status:04x}, request-id={request_id}")


if __name__ == "__main__":
    main()
