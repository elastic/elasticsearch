/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.CsvTestUtils.loadCsvSpecValues;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.APPROXIMATION;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.FORK_V9;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.METRICS_GROUP_BY_ALL;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.UNMAPPED_FIELDS;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.VIEWS_WITH_BRANCHING;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;

/**
 * Tests for FORK. We generate tests for FORK from existing CSV tests.
 * We append a `| FORK (WHERE true) (WHERE true) | WHERE _fork == "fork1" | DROP _fork` suffix to existing
 * CSV test cases. This will produce a query that executes multiple FORK branches but expects the same results
 * as the initial CSV test case.
 * For now, we skip tests that already require FORK, since multiple FORK commands are not allowed.
 */
public abstract class GenerativeForkRestTest extends EsqlSpecTestCase {
    public GenerativeForkRestTest(
        String fileName,
        String groupName,
        String testName,
        Integer lineNumber,
        CsvSpecReader.CsvTestCase testCase,
        String instructions
    ) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions);
    }

    @Override
    protected void doTest() throws Throwable {
        boolean addLimitAfterFork = randomBoolean();

        String suffix = " | FORK (WHERE true) (WHERE true) ";
        suffix = addLimitAfterFork ? suffix + " | LIMIT 300 " : suffix;

        suffix = suffix + "| WHERE _fork == \"fork1\" | DROP _fork";

        String query = testCase.query + suffix;
        doTest(query);
    }

    @Override
    protected void shouldSkipTest(String testName) throws IOException {
        super.shouldSkipTest(testName);

        assumeFalse(
            "Tests using FORK are skipped since we don't support multiple FORKs",
            testCase.requiredCapabilities.contains(FORK_V9.capabilityName())
        );

        assumeFalse(
            "Tests using INSIST are not supported for now",
            testCase.requiredCapabilities.contains(UNMAPPED_FIELDS.capabilityName())
        );

        assumeFalse(
            "Tests using subqueries are skipped since we don't support nested subqueries",
            testCase.requiredCapabilities.contains(SUBQUERY_IN_FROM_COMMAND.capabilityName())
        );

        assumeFalse(
            "Tests using PROMQL are not supported for now",
            testCase.requiredCapabilities.contains(EsqlCapabilities.Cap.PROMQL_COMMAND_V0.capabilityName())
        );

        assumeFalse(
            "Tests using GROUP_BY_ALL are skipped since we add a new _timeseries field",
            testCase.requiredCapabilities.contains(METRICS_GROUP_BY_ALL.capabilityName())
        );

        assumeFalse(
            "Tests using query approximation are skipped since query approximation is not supported with FORK",
            testCase.requiredCapabilities.contains(APPROXIMATION.capabilityName())
        );

        assumeFalse(
            "Tests using VIEWS not supported for now (until we merge VIEWS and Subqueries/FORK including branch merging)",
            testCase.requiredCapabilities.contains(VIEWS_WITH_BRANCHING.capabilityName())
        );

        assumeTrue("Cluster needs to support FORK", hasCapabilities(adminClient(), List.of(FORK_V9.capabilityName())));

        assumeFalse(
            "Tests expecting a _fork column can't be tested as _fork will be dropped",
            loadCsvSpecValues(testCase.expectedResults).columnNames().contains("_fork")
        );
    }
}
