/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.xpack.esql.CsvSpecReader;
import org.elasticsearch.xpack.esql.qa.rest.EsqlSpecTestCase;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.*;
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
        String query = testCase.query + " | FORK (WHERE true) (WHERE true) | WHERE _fork == \"fork1\" | DROP _fork";
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

        assumeFalse("Tests using PROMQL are not supported for now", testCase.requiredCapabilities.contains(PROMQL_V0.capabilityName()));

        assumeTrue("Cluster needs to support FORK", hasCapabilities(adminClient(), List.of(FORK_V9.capabilityName())));
    }
}
