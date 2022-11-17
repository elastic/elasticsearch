/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;

public class ESClientYamlSuiteTestCaseFailLogIT extends ESClientYamlSuiteTestCase {

    public ESClientYamlSuiteTestCaseFailLogIT(final ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @TestLogging(
        reason = "testing logging on yaml test failure",
        value = "org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCaseFailIT:INFO"
    )
    @Override
    public void test() throws IOException {
        final MockLogAppender mockLogAppender = new MockLogAppender();
        try {
            mockLogAppender.start();
            Loggers.addAppender(LogManager.getLogger(ESClientYamlSuiteTestCaseFailLogIT.class), mockLogAppender);

            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "message with dump of the test yaml",
                    ESClientYamlSuiteTestCaseFailLogIT.class.getCanonicalName(),
                    Level.INFO,
                    "Dump test yaml [*10_fail.yml] on failure:*Hello: testid*Hello: test2id*"
                )
            );

            mockLogAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "message with stash dump of response",
                    ESClientYamlSuiteTestCaseFailLogIT.class.getCanonicalName(),
                    Level.INFO,
                    "Stash dump on test failure [{*Hello: testid*}]"
                )
            );

            try {
                super.test();
            } catch (AssertionError error) {
                // If it is the error we expect, ignore it, else re-throw.
                if (error.getMessage().contains("foo: expected \"Hello: test2id\" but was \"Hello: testid\"") == false) {
                    throw error;
                }
            }

            mockLogAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(LogManager.getLogger(ESClientYamlSuiteTestCaseFailLogIT.class), mockLogAppender);
            mockLogAppender.stop();
        }
    }
}
