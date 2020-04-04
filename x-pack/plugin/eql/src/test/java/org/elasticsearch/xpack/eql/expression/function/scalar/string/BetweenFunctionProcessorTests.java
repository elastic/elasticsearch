/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.apache.directory.api.util.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;

import static org.hamcrest.Matchers.equalTo;

public class BetweenFunctionProcessorTests extends ESTestCase {
    public void testNullOrEmptyParameters() throws Exception {
        String left = randomBoolean() ? null : randomAlphaOfLength(10);
        String right = randomBoolean() ? null : randomAlphaOfLength(10);
        Boolean greedy = randomBoolean() ? null : randomBoolean();
        Boolean caseSensitive = randomBoolean() ? null : randomBoolean();

        String source = randomBoolean() ? null : Strings.EMPTY_STRING;

        // The source parameter can be null. Expect exception if any of other parameters is null.
        if ((source != null) && (left == null || right == null || greedy == null || caseSensitive == null)) {
            EqlIllegalArgumentException e = expectThrows(EqlIllegalArgumentException.class,
                    () -> BetweenFunctionProcessor.doProcess(source, left, right, greedy, caseSensitive));
            if (left == null || right == null) {
                assertThat(e.getMessage(), equalTo("A string/char is required; received [null]"));
            } else {
                assertThat(e.getMessage(), equalTo("A boolean is required; received [null]"));
            }
        } else {
            assertThat(BetweenFunctionProcessor.doProcess(source, left, right, greedy, caseSensitive), equalTo(source));
        }
    }
}
