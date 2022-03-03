/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.util.StringUtils;

import static org.hamcrest.Matchers.equalTo;

public class BetweenFunctionProcessorTests extends ESTestCase {
    public void testNullOrEmptyParameters() throws Exception {
        String left = randomBoolean() ? null : randomAlphaOfLength(10);
        String right = randomBoolean() ? null : randomAlphaOfLength(10);
        Boolean greedy = randomBoolean() ? null : randomBoolean();
        Boolean caseInsensitive = randomBoolean() ? null : randomBoolean();

        String source = randomBoolean() ? null : StringUtils.EMPTY;

        // The source parameter can be null. Expect exception if any of other parameters is null.
        if ((source != null) && (left == null || right == null || greedy == null || caseInsensitive == null)) {
            QlIllegalArgumentException e = expectThrows(
                QlIllegalArgumentException.class,
                () -> BetweenFunctionProcessor.doProcess(source, left, right, greedy, caseInsensitive)
            );
            if (left == null || right == null) {
                assertThat(e.getMessage(), equalTo("A string/char is required; received [null]"));
            } else {
                assertThat(e.getMessage(), equalTo("A boolean is required; received [null]"));
            }
        } else {
            assertThat(BetweenFunctionProcessor.doProcess(source, left, right, greedy, caseInsensitive), equalTo(source));
        }
    }
}
