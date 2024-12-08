/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;

import java.util.Locale;
import java.util.concurrent.Callable;

import static org.elasticsearch.xpack.eql.expression.function.scalar.string.StringContainsFunctionProcessor.doProcess;
import static org.hamcrest.Matchers.equalTo;

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

            // The string parameter can be null. Expect exception if any of other parameters is null.
            if (string != null && substring == null) {
                EqlIllegalArgumentException e = expectThrows(
                    EqlIllegalArgumentException.class,
                    () -> doProcess(string, substring, insensitive)
                );
                assertThat(e.getMessage(), equalTo("A string/char is required; received [null]"));
            } else {
                assertThat(doProcess(string, substring, insensitive), equalTo(string == null ? null : true));

                // deliberately make the test return "false/true" by lowercasing or uppercasing the substring in a in/sensitive scenario
                if (substring != null) {
                    String subsChanged = randomBoolean() ? substring.toLowerCase(Locale.ROOT) : substring.toUpperCase(Locale.ROOT);
                    if (substring.equals(subsChanged) == false) {
                        assertThat(doProcess(string, subsChanged, insensitive), equalTo(string == null ? null : insensitive));
                    }
                }
            }

            return null;
        });
    }

}
