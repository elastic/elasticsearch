/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest.yaml;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;

public class ESClientYamlSuiteTestCaseMultipleFailuresIT extends ESClientYamlSuiteTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local().build();

    public ESClientYamlSuiteTestCaseMultipleFailuresIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        try {
            return createParameters("20_multiple_failures");
        } catch (AssertionError e) {
            // Missing file on BWC tests, to be removed in the next PR.
            return Collections.emptyList();
        }
    }

    @TestLogging(
        reason = "testing logging on yaml test failure with multiple failures",
        value = "org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCaseMultipleFailuresIT:INFO"
    )
    @Override
    public void test() throws IOException {
        try {
            super.test();
        } catch (AssertionError error) {
            String message = error.getMessage();
            assertThat("Error message should start with failure count", message, containsString("There were 3 errors"));
            assertThat("Error message should contain first failure", message, containsString("wrong_value1"));
            assertThat("Error message should contain second failure", message, containsString("wrong_value2"));
            assertThat("Error message should contain third failure", message, containsString("wrong_value3"));
            assertThat("Error message should contain actual value1", message, containsString("value1"));
            assertThat("Error message should contain actual value2", message, containsString("value2"));
            assertThat("Error message should contain actual value3", message, containsString("value3"));
        }
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
