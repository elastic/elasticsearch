/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.xpack.esql.CsvSpecReader;

import java.util.List;

import static org.elasticsearch.xpack.esql.CsvTestUtils.loadCsvSpecValues;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.APPROXIMATION_V7;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.ESQL_WITHOUT_GROUPING;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.FORK_V9;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.METRICS_GROUP_BY_ALL;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_WITH_LOOKUP_JOIN;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.OPTIONAL_FIELDS_UNMAPPED_LOAD_NULL_FALLBACK;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.PROMQL_COMMAND_V0;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITH_VIEW;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 * Shared skip-condition logic for tests that wrap csv-spec queries with FORK.
 */
public class ForkTestUtils {
    private ForkTestUtils() {}

    /**
     * Applies all assume conditions that rule out a csv-spec test case from being
     * run with an appended FORK suffix. Callers should invoke this from
     * {@code shouldSkipTest} (to skip before the test starts) or inside a
     * try/catch for {@link org.junit.AssumptionViolatedException} (to skip a
     * sub-variant silently without failing the whole test).
     */
    public static void shouldSkipForkTest(CsvSpecReader.CsvTestCase testCase, RestClient adminClient) {
        assumeFalse(
            "Tests using FORK are skipped since we don't support multiple FORKs",
            testCase.requiredCapabilities.contains(FORK_V9.capabilityName())
        );

        // FORK is not supported with unmapped_fields="load", see https://github.com/elastic/elasticsearch/issues/142033
        assumeFalse(
            "FORK is not supported with unmapped_fields=\"load\"",
            testCase.requiredCapabilities.contains(OPTIONAL_FIELDS_V5.capabilityName())
                || testCase.requiredCapabilities.contains(OPTIONAL_FIELDS_LOAD_WITH_LOOKUP_JOIN.capabilityName())
                || testCase.requiredCapabilities.contains(OPTIONAL_FIELDS_UNMAPPED_LOAD_NULL_FALLBACK.capabilityName())
        );
        assumeFalse(
            "Tests using subqueries are skipped since nested fork/subquery is not supported yet",
            testCase.requiredCapabilities.contains(SUBQUERY_IN_FROM_COMMAND.capabilityName())
        );

        assumeFalse(
            "Tests using subqueries are skipped since nested fork/subquery is not supported yet",
            testCase.requiredCapabilities.contains(WHERE_IN_SUBQUERY_WITHOUT_VIEW.capabilityName())
        );

        assumeFalse(
            "Tests using subqueries are skipped since nested fork/subquery/view is not supported yet",
            testCase.requiredCapabilities.contains(WHERE_IN_SUBQUERY_WITH_VIEW.capabilityName())
        );

        assumeFalse(
            "Tests using PROMQL are not supported for now",
            testCase.requiredCapabilities.contains(PROMQL_COMMAND_V0.capabilityName())
        );

        assumeFalse(
            "Tests using GROUP_BY_ALL are skipped since we add a new _timeseries field",
            testCase.requiredCapabilities.contains(METRICS_GROUP_BY_ALL.capabilityName())
        );

        assumeFalse(
            "FORK with ESQL WITHOUT grouping is not supported yet (plan consistency)",
            testCase.requiredCapabilities.contains(ESQL_WITHOUT_GROUPING.capabilityName())
        );

        assumeFalse(
            "Tests using query approximation are skipped since they contain FORKs",
            testCase.requiredCapabilities.contains(APPROXIMATION_V7.capabilityName())
                || testCase.requiredCapabilitiesLocalCluster.contains(APPROXIMATION_V7.capabilityName())
        );

        assumeFalse(
            "Tests using VIEWS not supported for now (until we merge VIEWS and Subqueries/FORK including branch merging)",
            testCase.requiredCapabilities.contains(VIEWS_WITH_BRANCHING.capabilityName())
        );

        assumeTrue("Cluster needs to support FORK", hasCapabilities(adminClient, List.of(FORK_V9.capabilityName())));

        assumeFalse(
            "Tests expecting a _fork column can't be tested as _fork will be dropped",
            loadCsvSpecValues(testCase.expectedResults).columnNames().contains("_fork")
        );
    }
}
