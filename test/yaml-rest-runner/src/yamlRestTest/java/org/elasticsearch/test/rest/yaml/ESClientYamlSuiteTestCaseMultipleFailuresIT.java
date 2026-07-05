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
import org.junit.runners.model.MultipleFailureException;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

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
            assertThat(error.getCause(), instanceOf(MultipleFailureException.class));
            MultipleFailureException cause = (MultipleFailureException) error.getCause();
            assertThat(cause.getFailures(), hasSize(3));
            assertThat(
                cause.getFailures(),
                containsInAnyOrder(
                    hasToString(containsString("wrong_value1")),
                    hasToString(containsString("wrong_value2")),
                    hasToString(containsString("wrong_value3"))
                )
            );
        }
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
