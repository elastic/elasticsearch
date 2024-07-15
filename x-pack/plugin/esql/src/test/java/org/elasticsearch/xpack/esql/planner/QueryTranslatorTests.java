/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.index.EsIndex;
import org.elasticsearch.xpack.esql.core.index.IndexResolution;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.optimizer.TestPlannerOptimizer;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.stats.Metrics;
import org.hamcrest.Matcher;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.matchesRegex;

public class QueryTranslatorTests extends ESTestCase {

    private static TestPlannerOptimizer plannerOptimizer;

    private static Analyzer makeAnalyzer(String mappingFileName) {
        var mapping = loadMapping(mappingFileName);
        EsIndex test = new EsIndex("test", mapping, Set.of("test"));
        IndexResolution getIndexResult = IndexResolution.valid(test);

        return new Analyzer(
            new AnalyzerContext(EsqlTestUtils.TEST_CFG, new EsqlFunctionRegistry(), getIndexResult, new EnrichResolution()),
            new Verifier(new Metrics())
        );
    }

    @BeforeClass
    public static void init() {
        plannerOptimizer = new TestPlannerOptimizer(EsqlTestUtils.TEST_CFG, makeAnalyzer("mapping-all-types.json"));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    public void assertQueryTranslation(String query, Matcher<String> translationMatcher) {
        PhysicalPlan optimized = plannerOptimizer.plan(query);
        EsQueryExec eqe = (EsQueryExec) optimized.collectLeaves().get(0);
        final String translatedQuery = eqe.query().toString().replaceAll("\\s+", "");
        assertThat(translatedQuery, translationMatcher);
    }

    public void testBinaryComparisons() {
        assertQueryTranslation("""
            FROM test | WHERE 10 < integer""", containsString("""
            "esql_single_value":{"field":"integer","next":{"range":{"integer":{"gt":10,"""));

        assertQueryTranslation("""
            FROM test | WHERE 10.0 <= double""", containsString("""
            esql_single_value":{"field":"double","next":{"range":{"double":{"gte":10.0,"""));

        assertQueryTranslation("""
            FROM test | WHERE "2007-12-03T10:15:30+01:00" > date""", containsString("""
            "esql_single_value":{"field":"date","next":{"range":{"date":{"lt":"2007-12-03T09:15:30.000Z","time_zone":"Z","""));

        assertQueryTranslation("""
            FROM test | WHERE 2147483648::unsigned_long > unsigned_long""", containsString("""
            "esql_single_value":{"field":"unsigned_long","next":{"range":{"unsigned_long":{"lt":2147483648,"""));

        assertQueryTranslation("""
            FROM test | WHERE 2147483648 >= long""", containsString("""
            "esql_single_value":{"field":"long","next":{"range":{"long":{"lte":2147483648,"""));

        assertQueryTranslation("""
            FROM test | WHERE "1.2.3" == version""", containsString("""
            "esql_single_value":{"field":"version","next":{"term":{"version":{"value":"1.2.3"}"""));

        assertQueryTranslation("""
            FROM test | WHERE "foo" == keyword""", containsString("""
            "esql_single_value":{"field":"keyword","next":{"term":{"keyword":{"value":"foo"}"""));

        assertQueryTranslation("""
            FROM test | WHERE "2007-12-03T10:15:30+01:00" == date""", containsString("""
            "esql_single_value":{"field":"date","next":{"term":{"date":{"value":"2007-12-03T09:15:30.000Z"""));

        assertQueryTranslation("""
            FROM test | WHERE ip != "127.0.0.1\"""", containsString("""
            "esql_single_value":{"field":"ip","next":{"bool":{"must_not":[{"term":{"ip":{"value":"127.0.0.1"}"""));
    }

    public void testRanges() {
        // Note: Currently binary comparisons are not combined into range queries, so we get bool queries with multiple
        // one-sided ranges for now.

        // Once we combine binary comparisons, this query should be trivial.
        assertQueryTranslation("""
            FROM test | WHERE 10 < integer OR integer < 12""", matchesRegex("""
            .*should.*""" + """
            esql_single_value":\\{"field":"integer".*"range":\\{"integer":\\{"gt":10,.*""" + """
            esql_single_value":\\{"field":"integer".*"range":\\{"integer":\\{"lt":12.*"""));

        assertQueryTranslation("""
            FROM test | WHERE 10 < integer AND integer < 12""", matchesRegex("""
            .*must.*esql_single_value":\\{"field":"integer\"""" + """
            .*"range":\\{"integer":\\{"gt":10,.*"range":\\{"integer":\\{"lt":12.*"""));

        assertQueryTranslation("""
            FROM test | WHERE 10 <= integer AND integer <= 12""", matchesRegex("""
            .*must.*esql_single_value":\\{"field":"integer\"""" + """
            .*"range":\\{"integer":\\{"gte":10,.*"range":\\{"integer":\\{"lte":12.*"""));

        assertQueryTranslation("""
            FROM test | WHERE 10.9 < double AND double < 12.1""", matchesRegex("""
            .*must.*esql_single_value":\\{"field":"double\"""" + """
            .*"range":\\{"double":\\{"gt":10.9,.*"range":\\{"double":\\{"lt":12.1.*"""));

        assertQueryTranslation("""
            FROM test | WHERE 10.9 <= double AND double <= 12.1""", matchesRegex("""
            .*must.*esql_single_value":\\{"field":"double\"""" + """
            .*"range":\\{"double":\\{"gte":10.9,.*"range":\\{"double":\\{"lte":12.1.*"""));

        assertQueryTranslation("""
            FROM test | WHERE "2007-12-03T10:15:30+01:00" < date AND date < "2024-01-01T10:15:30+01:00\"""", matchesRegex("""
            .*must.*esql_single_value":\\{"field":"date\"""" + """
            .*"range":\\{"date":\\{"gt":\"2007-12-03T09:15:30.000Z\",.*"range":\\{"date":\\{"lt":\"2024-01-01T09:15:30.000Z\".*"""));

        assertQueryTranslation("""
            FROM test | WHERE "2007-12-03T10:15:30+01:00" <= date AND date <= "2024-01-01T10:15:30+01:00\"""", matchesRegex("""
            .*must.*esql_single_value":\\{"field":"date\"""" + """
            .*"range":\\{"date":\\{"gte":\"2007-12-03T09:15:30.000Z\",.*"range":\\{"date":\\{"lte":\"2024-01-01T09:15:30.000Z\".*"""));

        assertQueryTranslation("""
            FROM test | WHERE 2147483648::unsigned_long < unsigned_long AND unsigned_long < 2147483650::unsigned_long""", matchesRegex("""
            .*must.*esql_single_value":\\{"field":"unsigned_long\"""" + """
            .*"range":\\{"unsigned_long":\\{"gt":2147483648,.*"range":\\{"unsigned_long":\\{"lt":2147483650,.*"""));

        assertQueryTranslation("""
            FROM test | WHERE 2147483648::unsigned_long <= unsigned_long AND unsigned_long <= 2147483650::unsigned_long""", matchesRegex("""
            .*must.*esql_single_value":\\{"field":"unsigned_long\"""" + """
            .*"range":\\{"unsigned_long":\\{"gte":2147483648,.*"range":\\{"unsigned_long":\\{"lte":2147483650,.*"""));
    }
}
