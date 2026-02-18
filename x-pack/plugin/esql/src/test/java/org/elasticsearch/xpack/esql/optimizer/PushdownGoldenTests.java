/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

import java.util.EnumSet;

public class PushdownGoldenTests extends GoldenTestCase {
    private static final EnumSet<Stage> STAGES = EnumSet.of(Stage.LOCAL_PHYSICAL_OPTIMIZATION);

    public void testFilterPushdownNoUnmapped() {
        String query = """
            FROM sample_data
            | KEEP message
            | WHERE message == "Connection error?"
            | SORT message
            """;
        runGoldenTest(query, STAGES);
    }

    public void testFilterNoPushdownWithUnmapped() {
        String query = """
            FROM sample_data
            | KEEP message, does_not_exist
            | WHERE does_not_exist::KEYWORD == "Connection error?"
            | SORT message
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

    private void runUnmappedTests(String query) {
        assumeTrue("Requires OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW", EsqlCapabilities.Cap.OPTIONAL_FIELDS_NULLIFY_TECH_PREVIEW.isEnabled());
        assumeTrue("Requires OPTIONAL_FIELDS", EsqlCapabilities.Cap.OPTIONAL_FIELDS.isEnabled());
        runGoldenTest("SET unmapped_fields=\"nullify\"; " + query, STAGES, "nullify");
        runGoldenTest("SET unmapped_fields=\"load\"; " + query, STAGES, "load");
    }
}
