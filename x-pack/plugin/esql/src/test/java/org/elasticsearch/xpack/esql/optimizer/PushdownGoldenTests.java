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

    public void testStartsWithOnMetadataIndex() {
        String query = """
            FROM sample_data METADATA _index
            | WHERE starts_with(_index, "sample")
            | KEEP message
            """;
        runGoldenTest(query, STAGES);
    }

    public void testEndsWithOnMetadataIndex() {
        String query = """
            FROM sample_data METADATA _index
            | WHERE ends_with(_index, "data")
            | KEEP message
            """;
        runGoldenTest(query, STAGES);
    }

    public void testLikeOnMetadataIndex() {
        String query = """
            FROM sample_data METADATA _index
            | WHERE _index LIKE "sample*"
            | KEEP message
            """;
        runGoldenTest(query, STAGES);
    }

    public void testLikeSingleCharOnMetadataIndex() {
        String query = """
            FROM sample_data METADATA _index
            | WHERE _index LIKE "sample_dat?"
            | KEEP message
            """;
        runGoldenTest(query, STAGES);
    }

    public void testLikeListOnMetadataIndex() {
        String query = """
            FROM sample_data METADATA _index
            | WHERE _index LIKE ("sample*", "no_match*")
            | KEEP message
            """;
        runGoldenTest(query, STAGES);
    }

    public void testRlikeOnMetadataIndex() {
        String query = """
            FROM sample_data METADATA _index
            | WHERE _index RLIKE "sample_.*"
            | KEEP message
            """;
        runGoldenTest(query, STAGES);
    }

    public void testRlikeListOnMetadataIndex() {
        String query = """
            FROM sample_data METADATA _index
            | WHERE _index RLIKE ("sample_.*", "no_match.*")
            | KEEP message
            """;
        runGoldenTest(query, STAGES);
    }

    public void testStartsWithOnDashedIndex() {
        String query = """
            FROM k8s-downsampled METADATA _index
            | WHERE starts_with(_index, "k8s-")
            | KEEP cluster
            """;
        runGoldenTest(query, STAGES);
    }

    public void testEndsWithOnDashedIndex() {
        String query = """
            FROM k8s-downsampled METADATA _index
            | WHERE ends_with(_index, "-downsampled")
            | KEEP cluster
            """;
        runGoldenTest(query, STAGES);
    }

    public void testLikeOnDashedIndex() {
        String query = """
            FROM k8s-downsampled METADATA _index
            | WHERE _index LIKE "k8s-*"
            | KEEP cluster
            """;
        runGoldenTest(query, STAGES);
    }

    private void runUnmappedTests(String query) {
        runTestsNullifyAndLoad(query, STAGES);
    }
}
