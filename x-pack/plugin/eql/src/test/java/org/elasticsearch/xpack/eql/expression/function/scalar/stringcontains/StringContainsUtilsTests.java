/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.stringcontains;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import static org.elasticsearch.xpack.eql.expression.function.scalar.stringcontains.StringContainsUtils.stringContains;

public class StringContainsUtilsTests extends ESTestCase {

    protected static final int NUMBER_OF_TEST_RUNS = 20;

    private static void run(Callable<Void> callable) throws Exception {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            callable.call();
        }
    }

    public void testStringContainsWithNullOrEmpty() {
        List<Boolean> caseSensList = Arrays.asList(true, false);
        for (boolean caseSensitive: caseSensList) {
            assertFalse(stringContains(null, null, caseSensitive));
            assertFalse(stringContains(null, "", caseSensitive));
            assertFalse(stringContains("", null, caseSensitive));
        }
    }

    public void testStringContainsWithRandom() throws Exception {
        run(() -> {
            String needle = randomAlphaOfLength(10);
            String haystack = randomAlphaOfLength(10) + needle + randomAlphaOfLength(10);
            assertTrue(stringContains(haystack, needle, true));
            return null;
        });
    }
}
