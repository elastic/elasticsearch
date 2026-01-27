#!/usr/bin/env python3
import argparse
import base64
import hashlib
import hmac
import json
import sys
import uuid
from datetime import datetime, timezone
from urllib.parse import quote


def percent_encode(value: str) -> str:
    return quote(value, safe="-_.~")


def canonicalize(params: dict) -> str:
    items = sorted((k, v) for k, v in params.items() if v is not None)
    return "&".join(f"{percent_encode(k)}={percent_encode(v)}" for k, v in items)


def string_to_sign(method: str, canonical_query: str) -> str:
    return f"{method}&{percent_encode('/')}&{percent_encode(canonical_query)}"


def sign(secret: str, string_to_sign_value: str, algo: str) -> str:
    if algo == "HMAC-SHA1":
        digest = hmac.new((secret + "&").encode("utf-8"), string_to_sign_value.encode("utf-8"), hashlib.sha1).digest()
    elif algo == "HMAC-SHA256":
        digest = hmac.new((secret + "&").encode("utf-8"), string_to_sign_value.encode("utf-8"), hashlib.sha256).digest()
    else:
        raise ValueError(f"unsupported signature method: {algo}")
    return base64.b64encode(digest).decode("utf-8")


def build_params(args: argparse.Namespace) -> dict:
    timestamp = args.timestamp or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    nonce = args.nonce or str(uuid.uuid4())
    params = {
        "Action": "GetCallerIdentity",
        "Version": "2015-04-01",
        "Format": "JSON",
        "AccessKeyId": args.access_key_id,
        "SignatureMethod": args.signature_method,
        "SignatureVersion": "1.0",
        "SignatureNonce": nonce,
        "Timestamp": timestamp,
    }
    if args.security_token:
        params["SecurityToken"] = args.security_token
    return params


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate X-ES-IAM-Signed header for Aliyun STS GetCallerIdentity.")
    parser.add_argument("--access-key-id", required=True, help="RAM AccessKeyId")
    parser.add_argument("--access-key-secret", required=True, help="RAM AccessKeySecret")
    parser.add_argument("--security-token", default="", help="STS SecurityToken (optional)")
    parser.add_argument("--signature-method", default="HMAC-SHA1", choices=("HMAC-SHA1", "HMAC-SHA256"))
    parser.add_argument("--timestamp", default="", help="Override timestamp (ISO8601, e.g. 2025-01-01T00:00:00Z)")
    parser.add_argument("--nonce", default="", help="Override SignatureNonce (uuid)")
    parser.add_argument("--print-header", action="store_true", help="Print the full header name and value")
    args = parser.parse_args()

    params = build_params(args)
    canonical_query = canonicalize(params)
    sig = sign(args.access_key_secret, string_to_sign("GET", canonical_query), args.signature_method)
    params["Signature"] = sig

    payload = json.dumps(params, separators=(",", ":"), sort_keys=True)
    header_value = base64.b64encode(payload.encode("utf-8")).decode("utf-8")
    if args.print_header:
        print(f"X-ES-IAM-Signed: {header_value}")
    else:
        print(header_value)
    return 0


if __name__ == "__main__":
    sys.exit(main())
