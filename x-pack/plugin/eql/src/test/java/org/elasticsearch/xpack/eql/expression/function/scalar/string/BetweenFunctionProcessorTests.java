/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.util.StringUtils;


public class BetweenFunctionProcessorTests extends ESTestCase {
    public void testNullOrEmptyParameters() throws Exception {
        String left = randomBoolean() ? null : randomAlphaOfLength(10);
        String right = randomBoolean() ? null : randomAlphaOfLength(10);
        Boolean greedy = randomBoolean() ? null : randomBoolean();
        boolean caseSensitive = randomBoolean();

        String source = randomBoolean() ? null : StringUtils.EMPTY;

        assertNull(BetweenFunctionProcessor.doProcess(source, left, right, greedy, caseSensitive));
    }
}
