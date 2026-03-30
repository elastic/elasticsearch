/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;

import java.util.EnumSet;

public class PushExpressionToFieldLoadGoldenTests extends GoldenTestCase {

    public static final EsqlTestUtils.TestSearchStats SEARCH_STATS = new EsqlTestUtils.TestSearchStats();

    private void runGoldenTest(String query) {
        runGoldenTest(
            query,
            EnumSet.of(
                Stage.ANALYSIS,
                Stage.LOGICAL_OPTIMIZATION,
                Stage.PHYSICAL_OPTIMIZATION,
                Stage.LOCAL_PHYSICAL_OPTIMIZATION,
                Stage.NODE_REDUCE,
                Stage.NODE_REDUCE_LOCAL_PHYSICAL_OPTIMIZATION
            ),
            SEARCH_STATS
        );
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testLengthInEval() {
        runGoldenTest("""
            FROM employees
            | EVAL l = LENGTH(last_name)
            | SORT emp_no
            | LIMIT 10
            | KEEP l
            """);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testLengthInWhere() {
        runGoldenTest("""
            FROM employees
            | WHERE LENGTH(last_name) > 1
            """);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testLengthInStats() {
        runGoldenTest("""
            FROM employees
            | STATS l = SUM(LENGTH(last_name))
            """);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testLengthInEvalAfterManyRenames() {
        runGoldenTest("""
            FROM employees
            | EVAL l1 = last_name
            | EVAL l2 = l1
            | EVAL l3 = l2
            | EVAL l = LENGTH(l3)
            | SORT emp_no
            | LIMIT 10
            | KEEP l
            """);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testLengthInWhereAndEval() {
        runGoldenTest("""
            FROM employees
            | WHERE LENGTH(last_name) > 1
            | EVAL l = LENGTH(last_name)
            """);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testLengthPushdownZoo() {
        runGoldenTest("""
            FROM employees
            | EVAL a1 = LENGTH(last_name), a2 = LENGTH(last_name), a3 = LENGTH(last_name),
                   a4 = abs(LENGTH(last_name)) + a1 + LENGTH(first_name) * 3
            | WHERE a1 > 1 and LENGTH(last_name) > 1
            | STATS l = SUM(LENGTH(last_name)) + AVG(a3) + SUM(LENGTH(first_name))
            """);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testLengthInStatsTwice() {
        runGoldenTest("""
            FROM employees
            | STATS l = SUM(LENGTH(last_name)) + AVG(LENGTH(last_name))
            """);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testLengthTwoFields() {
        runGoldenTest("""
            FROM employees
            | STATS last_name = SUM(LENGTH(last_name)), first_name = SUM(LENGTH(first_name))
            """);
    }

    // ---- Vector function push tests ----
    // Note: rather than randomizing the functions like the main tests do, I just picked a function to use here.

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testVectorFunctionsReplaced() {
        runGoldenTest("""
            from all_types
            | eval s = v_cosine(dense_vector, [0.78354394, 0.089722395, 0.88437265])
            | sort s desc
            | limit 1
            """);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testVectorFunctionsReplacedWithTopN() {
        runGoldenTest("""
            from all_types
            | eval s = v_cosine(dense_vector, [0.78354394, 0.089722395, 0.88437265])
            | sort s desc
            | limit 1
            | keep s
            """);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testVectorFunctionsInWhere() {
        runGoldenTest("""
            from all_types
            | where v_cosine(dense_vector, [0.78354394, 0.089722395, 0.88437265]) > 0.5
            | keep dense_vector
            """);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testVectorFunctionsInStats() {
        runGoldenTest("""
            from all_types
            | stats count(*) where v_cosine(dense_vector, [0.78354394, 0.089722395, 0.88437265]) > 0.5
            """);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testVectorFunctionsUpdateIntermediateProjections() {
        runGoldenTest("""
            from all_types
            | keep *
            | mv_expand keyword
            | eval similarity = v_cosine(dense_vector, [0.78354394, 0.089722395, 0.88437265])
            | sort similarity desc, keyword asc
            | limit 1
            """);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testVectorFunctionsWithDuplicateFunctions() {
        // The SORT ensures all EVALs end up inside the data-node fragment.
        runGoldenTest("""
             from all_types
             | eval s1 = v_hamming([0.15351605, 0.47691387, 0.6555141], dense_vector),
                    s2 = v_hamming([0.15351605, 0.47691387, 0.6555141], dense_vector) * 2 / 3
             | where v_hamming([0.15351605, 0.47691387, 0.6555141], dense_vector) + 5
                    + v_cosine(dense_vector, [0.18848974, 0.56734943, 0.5018673]) > 0
             | eval r2 = v_cosine([0.18848974, 0.56734943, 0.5018673], dense_vector)
                    + v_cosine([0.19720656, 0.54328305, 0.5056714], dense_vector)
             | sort s1 desc
             | limit 10
             | keep s1, s2, r2
            """);
    }

    // ---- Aggregate Metric Double tests ----

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testAggregateMetricDouble() {
        runGoldenTest("FROM k8s-downsampled | STATS m = min(network.eth0.tx)");
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testAggregateMetricDoubleWithAvgAndOtherFunctions() {
        runGoldenTest("""
            from k8s-downsampled
            | STATS s = sum(network.eth0.tx), a = avg(network.eth0.tx)
            """);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testAggregateMetricDoubleTSCommand() {
        runGoldenTest("""
            TS k8s-downsampled |
            STATS m = max(max_over_time(network.eth0.tx)),
                  c = count(count_over_time(network.eth0.tx)),
                  a = avg(avg_over_time(network.eth0.tx))
            BY pod, time_bucket = BUCKET(@timestamp,5minute)
            """);
    }

    // ---- Reduction plan tests ----

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testReductionPlanForTopNWithPushedDownFunctions() {
        assumeTrue("Node reduction must be enabled", EsqlCapabilities.Cap.ENABLE_REDUCE_NODE_LATE_MATERIALIZATION.isEnabled());
        runGoldenTest("""
            FROM all_types
            | EVAL score = V_DOT_PRODUCT(dense_vector, [1.0, 2.0, 3.0])
            | SORT integer DESC
            | LIMIT 10
            | KEEP text, score
            """);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/145205")
    public void testReductionPlanForTopNWithPushedDownFunctionsInOrder() {
        assumeTrue("Node reduction must be enabled", EsqlCapabilities.Cap.ENABLE_REDUCE_NODE_LATE_MATERIALIZATION.isEnabled());
        runGoldenTest("""
            FROM all_types
            | EVAL fieldLength = LENGTH(text)
            | SORT fieldLength DESC
            | LIMIT 10
            | KEEP text, fieldLength
            """);
    }

    /*
    TODO: Currently, we cannot run golden test for local phsyiscal plans for subqueries since they generate multiple physical plans
    // ---- Fork test ----

    public void testPushableFunctionsInFork() {
        runGoldenTest("""
            from all_types
            | eval u = v_cosine(dense_vector, [4, 5, 6])
            | fork
                (eval s = length(text) | keep s, u, keyword)
                (eval t = v_dot_product(dense_vector, [1, 2, 3]) | keep t, u, keyword)
            | eval x = length(keyword)
            """);
    }

    // ---- Subquery test ----

    public void testPushableFunctionsInSubqueries() {
        assumeTrue("Subqueries are allowed", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        assumeTrue("Subqueries are allowed", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND_WITHOUT_IMPLICIT_LIMIT.isEnabled());

        runGoldenTest("""
            from all_types, (from all_types | eval s = length(text) | keep s)
            | eval t = v_dot_product(dense_vector, [1, 2, 3])
            | keep s, t
            """);
    }

    // ---- Lookup join test (Primaries check) ----

    public void testPushDownFunctionsLookupJoin() {
        // The SORT ensures the evals below are in the data-node fragment.
        runGoldenTest("""
            from test
            | eval s = length(first_name)
            | rename languages AS language_code
            | keep s, language_code, last_name
            | lookup join languages_lookup ON language_code
            | eval t = length(last_name)
            | eval u = length(language_name)
            | sort language_code
            | limit 10
            """);
    }

     */
}
