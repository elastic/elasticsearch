"""Turns the jinja-templated clickbench/operations/02_all_queries.json of the (private) rally-internal-tracks repository into a plain
JSON list of {"name", "query"} objects. We do not keep a copy of that file here, so fetch it first:

  gh api repos/elastic/rally-internal-tracks/contents/clickbench/operations/02_all_queries.json \
     -H "Accept: application/vnd.github.raw" > 02_all_queries.json

Usage: parse_rally_queries.py 02_all_queries.json rally_queries.json"""
import json, re, sys

src, out = sys.argv[1], sys.argv[2]
text = open(src).read()
text = re.sub(r"\{%.*?%\}", "", text, flags=re.S)  # jinja statements
text = re.sub(r"\{\{.*?\}\}", "null", text, flags=re.S)  # jinja expressions: only used for fields that we do not read
text = text.strip().rstrip(",")
queries = [{"name": o["name"], "query": o["query"]} for o in json.loads("[" + text + "]")]
json.dump(queries, open(out, "w"), indent=1)
print(f"{len(queries)} queries")
