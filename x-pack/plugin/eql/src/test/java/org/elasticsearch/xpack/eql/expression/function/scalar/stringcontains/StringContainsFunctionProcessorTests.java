/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.stringcontains;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;

import java.util.concurrent.Callable;

import static org.hamcrest.Matchers.equalTo;

public class StringContainsFunctionProcessorTests extends ESTestCase {

    // TODO (AM): consolidate with other functions tests in previous PRs where I already used this pattern once they are merged
    protected static final int NUMBER_OF_TEST_RUNS = 20;

    protected static void run(Callable<Void> callable) throws Exception {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            callable.call();
        }
    }

    public void testNullOrEmptyParameters() throws Exception {
        run(() -> {
            String needle = randomBoolean() ? null : randomAlphaOfLength(10);
            String str = randomBoolean() ? null : randomAlphaOfLength(10);
            if (str != null && needle != null) {
                str += needle;
                str += randomAlphaOfLength(10);
            }
            final String haystack = str;
            Boolean caseSensitive = randomBoolean() ? null : randomBoolean();

            // The haystack parameter can be null. Expect exception if any of other parameters is null.
            if ((haystack != null) && (needle == null || caseSensitive == null)) {
                EqlIllegalArgumentException e = expectThrows(EqlIllegalArgumentException.class,
                        () -> StringContainsFunctionProcessor.doProcess(haystack, needle, caseSensitive));
                if (needle == null) {
                    assertThat(e.getMessage(), equalTo("A string/char is required; received [null]"));
                } else {
                    assertThat(e.getMessage(), equalTo("A boolean is required; received [null]"));
                }
            } else {
                assertThat(StringContainsFunctionProcessor.doProcess(haystack, needle, caseSensitive),
                        equalTo(haystack == null? null : true));
            }
            return null;
        });
    }
}
