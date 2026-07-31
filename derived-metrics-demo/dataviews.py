#!/usr/bin/env python3
"""Ensures the demo's Kibana data views exist and point where they should.

Kibana rejects a second data view with the same name, so simply POSTing again after the destination
has been renamed leaves the old one in place, pointing at a stream that no longer exists. This
replaces a data view whose title has drifted rather than skipping it.
"""

import argparse
import base64
import json
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kibana", default="http://localhost:5601")
    parser.add_argument("--user", default="elastic-admin")
    parser.add_argument("--password", default="elastic-password")
    parser.add_argument("--data-stream", default="logs-derived-demo-default")
    parser.add_argument("--interval", default="10s")
    parser.add_argument("--label", default="demo", help="distinguishes the data views of one stream from another's")
    args = parser.parse_args()

    token = base64.b64encode(f"{args.user}:{args.password}".encode()).decode()
    headers = {"Content-Type": "application/json", "Authorization": f"Basic {token}", "kbn-xsrf": "true"}

    def call(method, path, body=None):
        request = urllib.request.Request(
            f"{args.kibana}{path}",
            data=json.dumps(body).encode() if body is not None else None,
            headers=headers,
            method=method,
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
        # DELETE answers with an empty body, which is not JSON.
        return json.loads(body) if body else {}

    existing = {
        o["attributes"].get("name"): (o["id"], o["attributes"]["title"])
        for o in call("GET", "/api/saved_objects/_find?type=index-pattern&per_page=100")["saved_objects"]
    }

    wanted = [
        (args.data_stream, f"{args.label} source stream"),
        (f"derived-metrics-{args.data_stream}-{args.interval}", f"{args.label} derived metrics"),
    ]

    for title, name in wanted:
        found = existing.get(name)
        if found and found[1] == title:
            print(f"    {name}: already points at {title}")
            continue
        if found:
            # The destination was renamed; Kibana would reject a duplicate name, so replace it.
            print(f"    {name}: retitling {found[1]} -> {title}")
            call("DELETE", f"/api/data_views/data_view/{found[0]}")
        try:
            # allowHidden matters for the destination, which is a hidden data stream.
            created = call(
                "POST",
                "/api/data_views/data_view",
                {"data_view": {"title": title, "name": name, "timeFieldName": "@timestamp", "allowHidden": True}},
            )
            print(f"    {name}: created {created['data_view']['id']}")
        except urllib.error.HTTPError as e:
            print(f"    {name}: could not create ({e.read().decode()[:120]})")


if __name__ == "__main__":
    main()
