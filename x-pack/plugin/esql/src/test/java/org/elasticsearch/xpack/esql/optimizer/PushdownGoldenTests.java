/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import java.util.EnumSet;

/**
 * Golden tests for filter and sort pushdown to Lucene.
 * New pushdown tests should be added here rather than in {@link LocalPhysicalPlanOptimizerTests}.
 */
public class PushdownGoldenTests extends UnmappedGoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOCAL_PHYSICAL_OPTIMIZATION);

    public void testFilterPushdownNoUnmapped() {
        String query = """
            FROM sample_data
            | KEEP message
            | WHERE message == "Connection error?"
            """;
        runGoldenTest(query, STAGES);
    }

    public void testFilterPushdownNoUnmappedFilterOnly() {
        String query = """
            FROM sample_data
            | KEEP message
            | WHERE message == "Connection error?"
            """;
        runGoldenTest(query, STAGES);
    }

    public void testFilterNoPushdownWithUnmapped() {
        String query = """
            FROM sample_data
            | KEEP message, does_not_exist
            | WHERE does_not_exist::KEYWORD == "Connection error?"
            """;
        runUnmappedTests(query);
    }

    public void testSortPushdownNoUnmapped() {
        String query = """
            FROM sample_data
            | KEEP message
            | SORT message
            | LIMIT 5
            """;
        runGoldenTest(query, STAGES);
    }

    public void testSortNoPushdownWithUnmapped() {
        String query = """
            FROM sample_data
            | KEEP message, does_not_exist
            | SORT does_not_exist
            | LIMIT 5
            """;
        runUnmappedTests(query);
    }

    public void testFilterConjunctionPushableAndNonPushable() {
        String query = """
            FROM sample_data
            | KEEP message, does_not_exist
            | WHERE message == "Connection error?" AND does_not_exist::KEYWORD == "foo"
            """;
        runUnmappedTests(query);
    }

    public void testFilterDisjunctionPushableAndNonPushable() {
        String query = """
            FROM sample_data
            | KEEP message, does_not_exist
            | WHERE message == "Connection error?" OR does_not_exist::KEYWORD == "foo"
            """;
        runUnmappedTests(query);
    }

    public void testSortConjunctionPushableAndNonPushable() {
        String query = """
            FROM sample_data
            | KEEP message, does_not_exist
            | SORT message, does_not_exist
            | LIMIT 5
            """;
        runUnmappedTests(query);
    }

    private void runUnmappedTests(String query) {
        runTestsNullifyAndLoad(query, STAGES);
    }
}
