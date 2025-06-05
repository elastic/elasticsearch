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

public abstract class GenerativeForkRestTest extends EsqlSpecTestCase  {
    public GenerativeForkRestTest(String fileName, String groupName, String testName, Integer lineNumber, CsvSpecReader.CsvTestCase testCase, String instructions, Mode mode) {
        super(fileName, groupName, testName, lineNumber, testCase, instructions, mode);
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
            "Tests using FORK or RRF already are skipped since we don't support multiple FORKs",
            testCase.requiredCapabilities.contains(FORK_V7.capabilityName()) || testCase.requiredCapabilities.contains(RRF.capabilityName())
        );

        assumeFalse(
                "Tests using INSIST are not supported for now",
                testCase.requiredCapabilities.contains(UNMAPPED_FIELDS.capabilityName())
        );

        assumeFalse(
                "Tests using implicit_casting_date_and_date_nanos are not supported for now",
                testCase.requiredCapabilities.contains(IMPLICIT_CASTING_DATE_AND_DATE_NANOS.capabilityName())
        );

        assumeTrue(
            "Cluster needs to support FORK",
            hasCapabilities(client(), List.of(FORK_V7.capabilityName()))
        );
    }
}
