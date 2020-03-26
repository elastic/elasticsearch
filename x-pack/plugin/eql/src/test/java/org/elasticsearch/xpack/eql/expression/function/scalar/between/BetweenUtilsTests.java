/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.between;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.Callable;

import static org.hamcrest.Matchers.equalTo;

public class BetweenUtilsTests extends ESTestCase {

    protected static final int NUMBER_OF_TEST_RUNS = 20;

    private static void run(Callable<Void> callable) throws Exception {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            callable.call();
        }
    }

    public void testNullOrEmptyString() throws Exception {
        run(() -> {
            String string = randomBoolean() ? null : "";
            String left = randomAlphaOfLength(10);
            String right = randomAlphaOfLength(10);
            boolean greedy = randomBoolean();
            boolean caseSensitive = randomBoolean();
            if (string == null) {
                assertNull(BetweenUtils.between(string, left, right, greedy, caseSensitive));
            } else {
                assertThat(BetweenUtils.between(string, left, right, greedy, caseSensitive), equalTo(string));
            }
            return null;
        });
    }


    public void testEmptyNullLeftRight() throws Exception {
        run(() -> {
            String string = randomAlphaOfLength(10);
            String left = randomBoolean() ? null : "";
            String right = randomBoolean() ? null : "";
            boolean greedy = randomBoolean();
            boolean caseSensitive = randomBoolean();
            assertThat(BetweenUtils.between(string, left, right, greedy, caseSensitive), equalTo(string));
            return null;
        });
    }


    // Test from EQL doc https://eql.readthedocs.io/en/latest/query-guide/functions.html
    public void testBasicEQLExamples() throws Exception {
        assertThat(BetweenUtils.between("welcome to event query language", " ", " ", false, false),
                equalTo("to"));
        assertThat(BetweenUtils.between("welcome to event query language", " ", " ", true, false),
                equalTo("to event query"));
        assertThat(BetweenUtils.between("System Idle Process", "s", "e", true, false),
                equalTo("ystem Idle Proc"));

        assertThat(BetweenUtils.between("C:\\workspace\\dev\\TestLogs\\something.json", "dev", ".json", false, false),
                equalTo("\\TestLogs\\something"));

        assertThat(BetweenUtils.between("C:\\workspace\\dev\\TestLogs\\something.json", "dev", ".json", true, false),
                equalTo("\\TestLogs\\something"));

        assertThat(BetweenUtils.between("System Idle Process", "s", "e", false, false),
                equalTo("yst"));
    }
}
