/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.expression.function.scalar.string.StringContainsFunctionProcessor;

import java.util.concurrent.Callable;

import static org.hamcrest.Matchers.equalTo;

public class StringContainsFunctionProcessorTests extends ESTestCase {

    protected static final int NUMBER_OF_TEST_RUNS = 20;

    protected static void run(Callable<Void> callable) throws Exception {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            callable.call();
        }
    }

    public void testNullOrEmptyParameters() throws Exception {
        run(() -> {
            String substring = randomBoolean() ? null : randomAlphaOfLength(10);
            String str = randomBoolean() ? null : randomAlphaOfLength(10);
            if (str != null && substring != null) {
                str += substring;
                str += randomAlphaOfLength(10);
            }
            final String string = str;

            // The string parameter can be null. Expect exception if any of other parameters is null.
            if ((string != null) && (substring == null)) {
                EqlIllegalArgumentException e = expectThrows(EqlIllegalArgumentException.class,
                        () -> StringContainsFunctionProcessor.doProcess(string, substring));
                assertThat(e.getMessage(), equalTo("A string/char is required; received [null]"));
            } else {
                assertThat(StringContainsFunctionProcessor.doProcess(string, substring),
                        equalTo(string == null ? null : true));
            }
            return null;
        });
    }
}
