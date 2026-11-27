/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;

public class ColumnarNullFailureReportingTests extends AbstractColumnarNullContractTestCase {

    public void testFailureNamesTheScenarioTheStrategyAndTheConfiguration() {
        final AssertionError error = expectThrows(
            AssertionError.class,
            () -> assertSourceDropsNullElements(neverEqual(), IndexingStrategy.DOCUMENT)
        );
        assertThat(error.getMessage(), containsString("configurations failed"));
        assertThat(error.getMessage(), containsString("keyword"));
        assertThat(error.getMessage(), containsString("DOCUMENT"));
        assertThat(error.getMessage(), containsString("sourceMode=COLUMNAR_STORED"));
        assertThat(error.getMessage(), containsString("document="));
    }

    public void testFailureKeepsEveryConfigurationAndTheOriginalAssertion() {
        final AssertionError error = expectThrows(
            AssertionError.class,
            () -> assertSourceDropsNullElements(neverEqual(), IndexingStrategy.DOCUMENT)
        );
        assertThat(error.getSuppressed().length, greaterThan(0));
        Throwable root = error;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        final String original = "assertSourceDropsNulls";
        assertTrue(
            "the original assertion is in the stack of the root cause",
            Arrays.stream(root.getStackTrace()).anyMatch(frame -> frame.getMethodName().equals(original))
        );
    }

    private static Scenario neverEqual() {
        final AtomicInteger calls = new AtomicInteger();
        return scenario("keyword", () -> "a").withCanonical(value -> String.valueOf(calls.incrementAndGet()));
    }
}
