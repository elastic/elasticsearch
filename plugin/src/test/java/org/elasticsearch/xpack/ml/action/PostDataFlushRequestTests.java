/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.xpack.ml.action.FlushJobAction.Request;
import org.elasticsearch.xpack.ml.support.AbstractStreamableTestCase;

public class PostDataFlushRequestTests extends AbstractStreamableTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAsciiOfLengthBetween(1, 20));
        request.setCalcInterim(randomBoolean());
        if (randomBoolean()) {
            request.setStart(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setEnd(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setAdvanceTime(randomAsciiOfLengthBetween(1, 20));
        }
        return request;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    public void testNullJobIdThrows() {
        expectThrows(IllegalArgumentException.class, () -> new Request(null));
    }
}