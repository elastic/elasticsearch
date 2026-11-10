# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".

"""Check attribution, corpus completeness, and dashboard-weighted threshold semantics."""
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("metrics", Path(__file__).with_name("promcheck-dashboard-metrics.py"))
metrics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(metrics)


class DashboardMetricsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.dashboards = [
            {"id": 1, "name": "One", "queries": ['up{job="$job"}', "rate(x[5m])", "rate(x[5m])"]},
            {"id": 2, "name": "Two", "queries": ['up{job="$job"}']},
            {"id": 3, "name": "Empty", "queries": []},
        ]
        self.cases = [{"query": 'up{job="promcheck"}', **params} for params in
                      ({"start": -3600, "end": 0, "step": 60}, {"time": 0})]

    def prepare(self):
        metrics.write_json(self.root / "source.json", self.dashboards)
        (self.root / "legacy.jsonl").write_text('// @set job promcheck\n')
        with tarfile.open(self.root / "bundle.tar.gz", "w:gz") as archive:
            for name, body in {"dataset.csv.gz": b"dataset bytes", "NOTICE": b"upstream license",
                               "queries.jsonl": b"".join((json.dumps(c) + "\n").encode() for c in self.cases)}.items():
                entry = tarfile.TarInfo(name)
                entry.size = len(body)
                archive.addfile(entry, io.BytesIO(body))
        metrics.prepare(self.root / "source.json", self.root / "legacy.jsonl", self.root / "bundle.tar.gz",
                        self.root / "recovered.tar.gz", self.root / "manifest.json", self.root / "audit.json", 3)
        return json.loads((self.root / "manifest.json").read_text())

    def verdicts(self, manifest):
        return [{"type": "query", "query": query, "instant": instant, "status": "match", "points": 1}
                for query in manifest["queries"] for instant in (False, True)]

    def report(self, rows):
        summaries = [{"type": "summary", "total": 4, "inspected": 4,
                      "match": sum(r.get("status") == "match" for r in rows)}]
        (self.root / "log.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows + summaries))
        metrics.report(self.root / "manifest.json", self.root / "log.jsonl",
                       self.root / "coverage.json", self.root / "details.json")
        return json.loads((self.root / "coverage.json").read_text())

    def test_recovers_both_variants_and_retains_dataset_and_license(self):
        self.prepare()
        audit = json.loads((self.root / "audit.json").read_text())
        self.assertEqual(audit, {"dashboards": 3, "empty_dashboards": 1, "raw_distinct_queries": 2,
                                "normalized_queries": 2, "release_queries": 1, "recovered_cases": 2,
                                "cases": 4, "missing_queries": 0, "unattributed_queries": 0})
        with tarfile.open(self.root / "recovered.tar.gz") as archive:
            self.assertEqual(archive.extractfile("dataset.csv.gz").read(), b"dataset bytes")
            self.assertEqual(archive.extractfile("NOTICE").read(), b"upstream license")
            cases = [json.loads(line) for line in archive.extractfile("queries.jsonl")]
        self.assertEqual(cases[:2], self.cases)
        self.assertEqual(cases[2:], [{"query": "rate(x[5m])", "start": -3600, "end": 0, "step": 60},
                                    {"query": "rate(x[5m])", "time": 0}])

    def test_recovers_a_single_missing_variant(self):
        self.cases.pop()
        self.prepare()
        self.assertEqual(json.loads((self.root / "audit.json").read_text())["recovered_cases"], 3)

    def test_shared_queries_credit_each_dashboard_once_and_keep_empty_in_denominator(self):
        manifest = self.prepare()
        rows = self.verdicts(manifest)
        # Range matches but instant fails: rate(x[5m]) is unsupported, not half-supported.
        next(r for r in rows if r["query"] == "rate(x[5m])" and r["instant"])["status"] = "mismatch"
        buckets = self.report(rows)
        self.assertEqual([b["threshold_pct"] for b in buckets], list(range(0, 101, 10)))
        self.assertEqual(buckets[5]["dashboard_count"], 2)  # >= 50% includes boundary.
        self.assertEqual(buckets[6]["dashboard_count"], 1)
        self.assertEqual(buckets[10]["dashboard_count"], 1)
        self.assertAlmostEqual(buckets[10]["dashboard_pct"], 100 / 3)
        self.assertEqual(buckets[0]["empty_dashboards"], 1)
        details = json.loads((self.root / "details.json").read_text())
        self.assertEqual(details[0]["query_total"], 2)  # Repeated panels don't overweight a query.
        self.assertIsNone(details[2]["support_pct"])

    def test_empty_matches_and_all_nonmatch_statuses_do_not_demonstrate_support(self):
        for status, points in (("match", 0), ("error", 1), ("skipped", 1), ("mismatch", 1)):
            with self.subTest(status=status, points=points):
                rows = self.verdicts(self.prepare())
                for row in rows:
                    row.update(status=status, points=points)
                buckets = self.report(rows)
                self.assertEqual(buckets[0]["dashboard_count"], 2)
                self.assertEqual(buckets[1]["dashboard_count"], 0)

    def test_preserves_literal_whitespace_and_numeric_backreferences(self):
        self.assertEqual(metrics.canonical('  sum(\nup{label="a  b"})  '), 'sum( up{label="a  b"})')
        self.assertEqual(metrics.substitute('label_replace(up, "x", "$1", "x", "(.*)")', {}),
                         'label_replace(up, "x", "$1", "x", "(.*)")')
        with self.assertRaisesRegex(ValueError, "No fixture default"):
            metrics.substitute('up{job="$new"}', {})

    def test_preserves_distinct_original_queries_collapsed_by_defaults(self):
        self.dashboards[0]["queries"] = ['up{job="$job"}', 'up{job="promcheck"}']
        manifest = self.prepare()
        self.assertEqual(len(manifest["dashboards"][0]["queries"]), 2)
        self.assertEqual(len(manifest["queries"]), 1)

    def test_rejects_unknown_and_duplicate_release_cases(self):
        for extra in ({"query": "not_in_source", "time": 0}, self.cases[0]):
            with self.subTest(extra=extra):
                self.cases.append(extra)
                with self.assertRaisesRegex(ValueError, "Unattributed or duplicate"):
                    self.prepare()
                self.cases.pop()

    def test_rejects_changed_or_duplicate_dashboard_population(self):
        self.dashboards[2]["id"] = 2
        with self.assertRaisesRegex(ValueError, "dashboard count"):
            self.prepare()
        self.dashboards.pop()
        with self.assertRaisesRegex(ValueError, "dashboard count"):
            self.prepare()

    def test_rejects_missing_duplicate_unexpected_and_malformed_verdicts(self):
        rows = self.verdicts(self.prepare())
        for changed in (rows[:-1], rows + [rows[0]], rows + [{**rows[0], "query": "unknown"}],
                        [{**rows[0], "points": -1}] + rows[1:], [{**rows[0], "instant": 0}] + rows[1:],
                        [{**rows[0], "status": "unknown"}] + rows[1:]):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                self.report(changed)

    def test_near_complete_dashboard_is_not_rounded_into_full_support(self):
        manifest = self.prepare()
        manifest["dashboards"][0]["queries"] = ['up{job="promcheck"}'] * 1000 + ["rate(x[5m])"]
        metrics.write_json(self.root / "manifest.json", manifest)
        rows = self.verdicts(manifest)
        next(r for r in rows if r["query"] == "rate(x[5m])")["status"] = "error"
        self.assertEqual(self.report(rows)[10]["dashboard_count"], 1)


if __name__ == "__main__":
    unittest.main()
