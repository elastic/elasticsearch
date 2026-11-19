/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.report;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataCatalog;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.WorkloadSpec;
import org.elasticsearch.xpack.esql.qa.publicdata.report.JUnitResults.Status;
import org.elasticsearch.xpack.esql.qa.publicdata.report.JUnitResults.TestResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The verdict threshold over the fixture catalog: the fixture has one workload corpus with two
 * active variants (6 tests, one -Ignore'd -> 5 enabled on the reference; a 4-test query_subset on
 * the shards leg, which excludes the multi-source twin) and one failure-only corpus (1 case) ->
 * expectation: 5 + 4 + 1 = 10 executions.
 */
public class VerdictTests extends ESTestCase {

    private static final String REF = "fixture-s3-parquet-snappy-single";
    private static final String SHARDS = "fixture-s3-csv-gzip-shards";
    private static final String DIRTY = "fixture-dirty-s3-csv-uncompressed-single";

    private static PublicDataCatalog catalog() {
        return PublicDataCatalog.loadFromClasspath("/fixture-catalog.yml");
    }

    private static Map<String, WorkloadSpec> workloads() {
        return Map.of("fixture-workload.csv-spec", WorkloadSpec.loadFromClasspath("/fixture-workload.csv-spec"));
    }

    private static List<TestResult> allGreen() {
        List<TestResult> results = new ArrayList<>();
        for (String test : List.of("q1_scan", "q2_agg", "q3_topn", "q4_limit")) {
            results.add(new TestResult(REF, test, Status.PASSED, null));
            results.add(new TestResult(SHARDS, test, Status.PASSED, null));
        }
        results.add(new TestResult(REF, "q2_aggMulti", Status.PASSED, null)); // not in the shards leg's query_subset
        results.add(new TestResult(REF, "q5_defect-Ignore", Status.SKIPPED, null)); // -Ignore'd: skips don't count
        results.add(new TestResult(DIRTY, "testFailsCleanly", Status.PASSED, null));
        return results;
    }

    public void testAllGreenPasses() {
        Verdict verdict = Verdict.evaluate(catalog(), workloads(), allGreen(), Set.of());
        assertEquals(verdict.problems().toString(), Verdict.Status.PASS, verdict.status());
        assertEquals(3, verdict.legs().size());
        assertTrue(verdict.legs().stream().allMatch(l -> l.outcome().equals("PASS")));
    }

    public void testCorrectnessFailureFails() {
        List<TestResult> results = new ArrayList<>(allGreen());
        results.removeIf(r -> r.variantLabel().equals(REF) && r.testName().equals("q2_agg"));
        results.add(new TestResult(REF, "q2_agg", Status.FAILED, "Data mismatch: expected <2> but was <3>"));
        Verdict verdict = Verdict.evaluate(catalog(), workloads(), results, Set.of());
        assertEquals(Verdict.Status.FAIL, verdict.status());
        assertEquals("FAIL", verdict.legs().stream().filter(l -> l.label().equals(REF)).findFirst().orElseThrow().outcome());
    }

    public void testInfraFailureIsAttributedSeparately() {
        List<TestResult> results = new ArrayList<>(allGreen());
        results.removeIf(r -> r.variantLabel().equals(SHARDS) && r.testName().equals("q3_topn"));
        results.add(new TestResult(SHARDS, "q3_topn", Status.FAILED, "INFRA_FAIL: exhausted 3 attempts of [q3_topn]"));
        Verdict verdict = Verdict.evaluate(catalog(), workloads(), results, Set.of());
        assertEquals(Verdict.Status.INFRA_FAIL, verdict.status());
        assertEquals("INFRA_FAIL", verdict.legs().stream().filter(l -> l.label().equals(SHARDS)).findFirst().orElseThrow().outcome());
    }

    public void testZeroExecutionCanNeverBeGreen() {
        Verdict verdict = Verdict.evaluate(catalog(), workloads(), List.of(), Set.of());
        assertEquals(Verdict.Status.INFRA_FAIL, verdict.status());
        assertFalse(verdict.problems().isEmpty());
    }

    public void testMissingLegFails() {
        List<TestResult> results = allGreen().stream().filter(r -> r.variantLabel().equals(DIRTY) == false).toList();
        Verdict verdict = Verdict.evaluate(catalog(), workloads(), results, Set.of());
        assertEquals(Verdict.Status.INFRA_FAIL, verdict.status());
        assertTrue(verdict.problems().toString(), verdict.problems().stream().anyMatch(p -> p.contains(DIRTY)));
    }

    public void testShortExecutedCountFails() {
        List<TestResult> results = allGreen().stream()
            .filter(r -> (r.variantLabel().equals(SHARDS) && r.testName().equals("q4_limit")) == false)
            .toList();
        Verdict verdict = Verdict.evaluate(catalog(), workloads(), results, Set.of());
        assertEquals(Verdict.Status.INFRA_FAIL, verdict.status());
        assertTrue(verdict.problems().toString(), verdict.problems().stream().anyMatch(p -> p.contains("executed 3")));
    }

    public void testPinDriftIsMaintenance() {
        List<TestResult> results = allGreen().stream().filter(r -> r.variantLabel().equals(SHARDS) == false).toList();
        Verdict verdict = Verdict.evaluate(catalog(), workloads(), results, Set.of(SHARDS));
        assertEquals(verdict.problems().toString(), Verdict.Status.MAINTENANCE, verdict.status());
        assertEquals("PIN_DRIFT", verdict.legs().stream().filter(l -> l.label().equals(SHARDS)).findFirst().orElseThrow().outcome());
    }

    public void testDriftedLegThatStillRanIsAProblem() {
        Verdict verdict = Verdict.evaluate(catalog(), workloads(), allGreen(), Set.of(SHARDS));
        assertTrue(verdict.problems().toString(), verdict.problems().stream().anyMatch(p -> p.contains("despite PIN_DRIFT")));
    }

    public void testRenderings() {
        Verdict verdict = Verdict.evaluate(catalog(), workloads(), allGreen(), Set.of());
        String json = VerdictWriter.toJson(verdict);
        assertTrue(json.contains("\"status\": \"PASS\""));
        assertTrue(json.contains("\"leg\": \"" + REF + "\""));
        String md = VerdictWriter.toAnnotationMarkdown(verdict);
        assertTrue(md.contains("**PASS**"));
        assertTrue(md.contains("| fixture | " + SHARDS + " | PASS | 4/4 | 0 |"));
    }

    public void testJUnitParsing() {
        String xml = """
            <testsuite>
            <testcase name="test {public-data:public-x.q1_scan{lbl-s3-parquet-snappy-single}}" classname="C" time="1.0"/>
            <testcase name="test {public-data:public-x.q2_agg{lbl-s3-parquet-snappy-single}}" classname="C" time="1.0">
              <failure message="Data mismatch: expected &lt;1&gt;" type="AssertionError">trace</failure>
            </testcase>
            <testcase name="test {public-data:public-x.q5_x-Ignore{lbl-s3-parquet-snappy-single}}" classname="C" time="0">
              <skipped/>
            </testcase>
            <testcase name="testFailsCleanly {dirty-s3-csv-uncompressed-single-zero-byte}" classname="F" time="2.0"/>
            <testcase name="test {csv-spec:other.q9}" classname="X" time="0"/>
            </testsuite>
            """;
        List<TestResult> results = new ArrayList<>();
        JUnitResults.parseFile(xml, results);
        assertEquals(4, results.size());
        assertEquals(Status.PASSED, results.get(0).status());
        assertEquals(Status.FAILED, results.get(1).status());
        assertTrue(results.get(1).failureMessage().contains("Data mismatch: expected <1>"));
        assertEquals(Status.SKIPPED, results.get(2).status());
        assertEquals("dirty-s3-csv-uncompressed-single-zero-byte", results.get(3).variantLabel());
    }
}
