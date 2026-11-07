/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;

import java.util.Locale;
import java.util.concurrent.Callable;

import static org.elasticsearch.xpack.eql.expression.function.scalar.string.StringContainsFunctionProcessor.doProcess;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class StringContainsFunctionProcessorTests extends ESTestCase {

    protected static final int NUMBER_OF_TEST_RUNS = 20;

    protected static void run(Callable<Void> callable) throws Exception {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            callable.call();
        }
    }

    public void testStringContains() throws Exception {
        run(() -> {
            String substring = randomBoolean() ? null : randomAlphaOfLength(10);
            String str = randomBoolean() ? null : randomValueOtherThan(substring, () -> randomAlphaOfLength(10));
            boolean insensitive = randomBoolean();
            if (str != null && substring != null) {
                str += substring;
                str += randomValueOtherThan(substring, () -> randomAlphaOfLength(10));
            }
            final String string = str;

            // Either parameter can be null. A null in either argument yields a null result rather than an exception,
            // matching the lenient null handling of the other EQL string functions (e.g. endsWith, indexOf).
            if (string == null || substring == null) {
                assertThat(doProcess(string, substring, insensitive), nullValue());
            } else {
                assertThat(doProcess(string, substring, insensitive), equalTo(true));

                // deliberately make the test return "false/true" by lowercasing or uppercasing the substring in a in/sensitive scenario
                String subsChanged = randomBoolean() ? substring.toLowerCase(Locale.ROOT) : substring.toUpperCase(Locale.ROOT);
                if (substring.equals(subsChanged) == false) {
                    assertThat(doProcess(string, subsChanged, insensitive), equalTo(insensitive));
                }
            }

            return null;
        });
    }

}
