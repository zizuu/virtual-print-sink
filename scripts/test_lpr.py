#!/usr/bin/env python3
import argparse
import socket
import sys
from pathlib import Path


def expect_ack(sock: socket.socket, label: str) -> None:
    value = sock.recv(1)
    if value != b"\x00":
        raise RuntimeError(f"{label}: expected ACK 0x00, got {value!r}")


def send_file_block(sock: socket.socket, command: int, protocol_name: str, data: bytes) -> None:
    sock.sendall(bytes([command]) + f"{len(data)} {protocol_name}\n".encode("ascii"))
    expect_ack(sock, f"header {protocol_name}")
    sock.sendall(data + b"\x00")
    expect_ack(sock, f"body {protocol_name}")


def main() -> None:
    default_port = 515 if sys.platform.startswith("win") else 1515
    parser = argparse.ArgumentParser(description="Send a simple RFC 1179 LPR job to Virtual Print Sink")
    parser.add_argument("file", type=Path, help="File to send")
    parser.add_argument("--port", type=int, default=default_port)
    parser.add_argument("--queue", default="virtual")
    args = parser.parse_args()

    data = args.file.read_bytes()
    host = socket.gethostname().split(".")[0] or "localhost"
    control = (
        f"H{host}\n"
        f"Ptest-user\n"
        f"J{args.file.name}\n"
        f"N{args.file.name}\n"
    ).encode("utf-8")

    with socket.create_connection(("127.0.0.1", args.port), timeout=10) as sock:
        sock.sendall(b"\x02" + args.queue.encode("ascii") + b"\n")
        expect_ack(sock, "receive-job")
        send_file_block(sock, 0x02, f"cfA001{host}", control)
        send_file_block(sock, 0x03, f"dfA001{host}", data)

    print(f"Sent {len(data)} bytes by LPR to 127.0.0.1:{args.port}/{args.queue}")


if __name__ == "__main__":
    main()
