/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.between;

import org.apache.directory.api.util.Strings;

import static org.hamcrest.Matchers.equalTo;

public class BetweenUtilsTests extends BetweenBaseTestCase {
    public void testNullOrEmptyString() throws Exception {
        run(() -> {
            String left = randomAlphaOfLength(10);
            String right = randomAlphaOfLength(10);
            boolean greedy = randomBoolean();
            boolean caseSensitive = randomBoolean();

            String string = randomBoolean() ? null : Strings.EMPTY_STRING;
            assertThat(BetweenUtils.between(string, left, right, greedy, caseSensitive), equalTo(string));
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
    public void testBasicEQLExamples() {
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
