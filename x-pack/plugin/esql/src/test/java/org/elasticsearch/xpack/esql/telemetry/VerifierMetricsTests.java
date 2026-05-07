/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Verifier;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.DISSECT;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.DROP;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.ENRICH;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.EVAL;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.FORK;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.FROM;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.GROK;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.INLINE_STATS;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.KEEP;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.LIMIT;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.LIMIT_BY;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.LOOKUP_JOIN;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.LOOKUP_JOIN_ON_EXPRESSION;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.MV_EXPAND;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.PROMQL;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.RENAME;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.ROW;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.SHOW;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.SORT;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.STATS;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.SUBQUERY;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.TS;
import static org.elasticsearch.xpack.esql.telemetry.FeatureMetric.WHERE;
import static org.elasticsearch.xpack.esql.telemetry.Metrics.FEATURES_PREFIX;

public class VerifierMetricsTests extends ESTestCase {

    public void testDissectQuery() {
        Counters c = esql("from employees | dissect concat(first_name, \" \", last_name) \"%{a} %{b}\"");
        assertMetrics(c, Map.of(DISSECT, 1L, FROM, 1L), Map.of("concat", 1L));
    }

    public void testEvalQuery() {
        Counters c = esql("from employees | eval name_len = length(first_name)");
        assertMetrics(c, Map.of(EVAL, 1L, FROM, 1L), Map.of("length", 1L));
    }

    public void testGrokQuery() {
        Counters c = esql("from employees | grok concat(first_name, \" \", last_name) \"%{WORD:a} %{WORD:b}\"");
        assertMetrics(c, Map.of(GROK, 1L, FROM, 1L), Map.of("concat", 1L));
    }

    public void testLimitQuery() {
        Counters c = esql("from employees | limit 2");
        assertMetrics(c, Map.of(LIMIT, 1L, FROM, 1L));
    }

    public void testLimitByQuery() {
        Counters c = esql("from employees | sort first_name | limit 3 by gender");
        assertMetrics(c, Map.of(LIMIT_BY, 1L, SORT, 1L, FROM, 1L));
    }

    public void testSortQuery() {
        Counters c = esql("from employees | sort first_name desc nulls first");
        assertMetrics(c, Map.of(SORT, 1L, FROM, 1L));
    }

    public void testStatsQuery() {
        Counters c = esql("from employees | stats l = max(languages)");
        assertMetrics(c, Map.of(STATS, 1L, FROM, 1L), Map.of("max", 1L));
    }

    public void testWhereQuery() {
        Counters c = esql("from employees | where languages > 2");
        assertMetrics(c, Map.of(WHERE, 1L, FROM, 1L));
    }

    public void testTwoWhereQuery() {
        Counters c = esql("from employees | where languages > 2 | limit 5 | sort first_name | where first_name == \"George\"");
        assertMetrics(c, Map.of(LIMIT, 1L, SORT, 1L, WHERE, 1L, FROM, 1L));
    }

    public void testTwoQueriesExecuted() {
        Metrics metrics = new Metrics(TEST_FUNCTION_REGISTRY, true, true);
        Verifier verifier = new Verifier(metrics, new XPackLicenseState(() -> 0L));
        esqlWithVerifier("""
               from employees
               | where languages > 2
               | limit 5
               | eval name_len = length(first_name)
               | sort first_name
               | limit 3
            """, verifier);
        esqlWithVerifier("""
              from employees
              | where languages > 2
              | sort first_name desc nulls first
              | dissect concat(first_name, " ", last_name) "%{a} %{b}"
              | grok concat(first_name, " ", last_name) "%{WORD:a} %{WORD:b}"
              | stats x = max(languages)
              | sort x
              | stats y = min(x) by x
            """, verifier);
        Counters c = metrics.stats();
        assertMetrics(
            c,
            Map.of(DISSECT, 1L, EVAL, 1L, GROK, 1L, LIMIT, 1L, SORT, 2L, STATS, 1L, WHERE, 2L, FROM, 2L),
            Map.of("length", 1L, "concat", 1L, "max", 1L, "min", 1L)
        );
    }

    public void testMultipleFunctions() {
        Metrics metrics = new Metrics(TEST_FUNCTION_REGISTRY, true, true);
        Verifier verifier = new Verifier(metrics, new XPackLicenseState(() -> 0L));
        esqlWithVerifier("""
               from employees
               | where languages > 2
               | limit 5
               | eval name_len = length(first_name), surname_len = length(last_name)
               | sort length(first_name)
               | limit 3
            """, verifier);

        Counters c = metrics.stats();
        assertMetrics(c, Map.of(EVAL, 1L, LIMIT, 1L, SORT, 1L, WHERE, 1L, FROM, 1L), Map.of("length", 1L));

        esqlWithVerifier("""
              from employees
              | where languages > 2
              | sort first_name desc nulls first
              | dissect concat(first_name, " ", last_name) "%{a} %{b}"
              | grok concat(first_name, " ", last_name) "%{WORD:a} %{WORD:b}"
              | eval name_len = length(first_name), surname_len = length(last_name)
              | stats x = max(languages)
              | sort x
              | stats y = min(x) by x
            """, verifier);
        c = metrics.stats();

        assertMetrics(
            c,
            Map.of(DISSECT, 1L, EVAL, 2L, GROK, 1L, LIMIT, 1L, SORT, 2L, STATS, 1L, WHERE, 2L, FROM, 2L),
            Map.of("length", 2L, "concat", 1L, "max", 1L, "min", 1L)
        );
    }

    public void testEnrich() {
        Counters c = esql("""
            from employees
            | sort emp_no
            | limit 1
            | eval x = to_string(languages)
            | enrich languages on x
            | keep emp_no, language_name""");
        assertMetrics(c, Map.of(EVAL, 1L, LIMIT, 1L, SORT, 1L, ENRICH, 1L, FROM, 1L, KEEP, 1L), Map.of("to_string", 1L));
    }

    public void testMvExpand() {
        Counters c = esql("""
            from employees
            | where emp_no == 10004
            | limit 1
            | keep emp_no, job
            | mv_expand job
            | where job LIKE \"*a*\"
            | limit 2
            | where job LIKE \"*a*\"
            | limit 3""");
        assertMetrics(c, Map.of(LIMIT, 1L, WHERE, 1L, MV_EXPAND, 1L, FROM, 1L, KEEP, 1L));
    }

    public void testShowInfo() {
        Counters c = esql("show info |  stats  a = count(*), b = count(*), c = count(*) |  mv_expand c");
        assertMetrics(c, Map.of(STATS, 1L, MV_EXPAND, 1L, SHOW, 1L), Map.of("count", 1L));
    }

    public void testRow() {
        Counters c = esql("row a = [\"1\", \"2\"] | enrich languages on a with a_lang = language_name");
        assertMetrics(c, Map.of(ENRICH, 1L, ROW, 1L));
    }

    public void testDropAndRename() {
        Counters c = esql("from employees | rename gender AS foo | stats bar = count(*) by foo | drop foo | sort bar | drop bar");
        assertMetrics(c, Map.of(SORT, 1L, STATS, 1L, FROM, 1L, DROP, 1L, RENAME, 1L), Map.of("count", 1L));
    }

    public void testKeep() {
        Counters c = esql("""
            from employees
            | keep emp_no, languages
            | where languages is null or emp_no <= 10030
            | where languages in (2, 3, emp_no)
            | keep languages""");
        assertMetrics(c, Map.of(WHERE, 1L, FROM, 1L, KEEP, 1L));
    }

    public void testCategorize() {
        Counters c = esql("""
            from employees
            | keep emp_no, languages, gender
            | where languages is null or emp_no <= 10030
            | STATS COUNT() BY CATEGORIZE(gender)""");
        assertMetrics(c, Map.of(STATS, 1L, WHERE, 1L, FROM, 1L, KEEP, 1L), Map.of("count", 1L, "categorize", 1L));
    }

    public void testInlineStatsStandalone() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        Counters c = esql("""
            from employees
            | inline stats max(salary) by gender
            | where languages is not null""");
        assertMetrics(c, Map.of(WHERE, 1L, FROM, 1L, INLINE_STATS, 1L), Map.of("max", 1L));
    }

    public void testInlineStatsWithOtherStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        Counters c = esql("""
            from employees
            | inline stats m = max(salary) by gender
            | where languages is not null
            | stats max(m) by languages""");
        assertMetrics(c, Map.of(STATS, 1L, WHERE, 1L, FROM, 1L, INLINE_STATS, 1L), Map.of("max", 1L));
    }

    public void testBinaryPlanAfterStats() {
        Counters c = esql("""
            from employees
            | eval language_code = languages
            | stats m = max(salary) by language_code
            | lookup join languages_lookup on language_code""");
        assertMetrics(c, Map.of(EVAL, 1L, STATS, 1L, FROM, 1L, LOOKUP_JOIN, 1L), Map.of("max", 1L));
    }

    public void testBinaryPlanAfterStatsExpressionJoin() {
        assumeTrue(
            "requires LOOKUP JOIN ON boolean expression capability",
            EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
        );
        Counters c = esql("""
            from employees
            | eval language_code = languages
            | stats m = max(salary) by language_code
            | rename language_code as language_code_left
            | lookup join languages_lookup on language_code_left >= language_code""");
        assertMetrics(c, Map.of(EVAL, 1L, STATS, 1L, FROM, 1L, RENAME, 1L, LOOKUP_JOIN_ON_EXPRESSION, 1L), Map.of("max", 1L));
    }

    public void testBinaryPlanAfterInlineStats() {
        assumeTrue("INLINE STATS required", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        Counters c = esql("""
            from employees
            | eval language_code = languages
            | inline stats m = max(salary) by language_code
            | lookup join languages_lookup on language_code""");
        assertMetrics(c, Map.of(EVAL, 1L, FROM, 1L, INLINE_STATS, 1L, LOOKUP_JOIN, 1L), Map.of("max", 1L));
    }

    public void testTimeSeriesAggregate() {
        assumeTrue("TS required", EsqlCapabilities.Cap.TS_COMMAND_V0.isEnabled());
        Counters c = esql("""
            TS k8s
            | STATS sum(avg_over_time(network.cost))""");
        assertMetrics(c, Map.of(STATS, 1L, TS, 1L), Map.of("sum", 1L, "avg_over_time", 1L));
    }

    public void testTimeSeriesNoAggregate() {
        assumeTrue("TS required", EsqlCapabilities.Cap.TS_COMMAND_V0.isEnabled());
        Counters c = esql("""
            TS metrics
            | KEEP salary""");
        assertMetrics(c, Map.of(TS, 1L, KEEP, 1L));
    }

    public void testBinaryPlanAfterSubqueryInFromCommand() {
        assumeTrue("requires SUBQUERY IN FROM capability", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());

        Counters c = esql("""
             from employees
                      , (from employees | stats max = max(salary) by languages)
                      , (from employees | stats min = min(salary) by languages)
            | where min > 0 and max < 100000
            """);
        assertMetrics(c, Map.of(EVAL, 1L, STATS, 1L, WHERE, 1L, FROM, 1L, SUBQUERY, 1L, FORK, 1L), Map.of("max", 1L, "min", 1L));
    }

    public void testPromql() {
        Counters c = esql("""
            PROMQL index=k8s step=5m sum(network.cost)""");
        assertMetrics(c, Map.of(PROMQL, 1L, TS, 1L));
    }

    private void assertMetrics(Counters c, Map<FeatureMetric, Long> expectedFeatures) {
        assertMetrics(c, expectedFeatures, Map.of());
    }

    /**
     * Asserts that every counter in {@code c} equals the value declared in {@code expectedFeatures} or
     * {@code expectedFunctions}, defaulting to {@code 0} for anything absent from the maps.
     */
    private void assertMetrics(Counters c, Map<FeatureMetric, Long> expectedFeatures, Map<String, Long> expectedFunctions) {
        for (FeatureMetric metric : FeatureMetric.values()) {
            long expectedValue = expectedFeatures.getOrDefault(metric, 0L);
            assertEquals("Counter [" + metric + "]", expectedValue, c.get(FEATURES_PREFIX + metric));
        }
        Map<?, ?> functionCounters = (Map<?, ?>) c.toNestedMap().get("functions");
        for (var entry : functionCounters.entrySet()) {
            String name = (String) entry.getKey();
            long expectedValue = expectedFunctions.getOrDefault(name, 0L);
            assertEquals("Function [" + name + "]", expectedValue, ((Number) entry.getValue()).longValue());
        }
        for (String name : expectedFunctions.keySet()) {
            assertNotNull("Expected function [" + name + "] is not a registered function", functionCounters.get(name));
        }
    }

    private Counters esql(String esql) {
        return esql(esql, null);
    }

    private void esqlWithVerifier(String esql, Verifier verifier) {
        esql(esql, verifier);
    }

    private Counters esql(String esql, Verifier v) {
        Verifier verifier = v;
        Metrics metrics = null;
        if (v == null) {
            metrics = new Metrics(TEST_FUNCTION_REGISTRY, true, true);
            verifier = new Verifier(metrics, new XPackLicenseState(() -> 0L));
        }
        analyzer().addIndex("metrics", "mapping-basic.json", IndexMode.TIME_SERIES)
            .addK8s()
            .addEmployees()
            .addAnalysisTestsEnrichResolution()
            .addLanguagesLookup()
            .buildAnalyzer(verifier)
            .analyze(TEST_PARSER.parseQuery(esql));

        return metrics == null ? null : metrics.stats();
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
