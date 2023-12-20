/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction.Request;

public class PostDataFlushRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAlphaOfLengthBetween(1, 20));
        request.setCalcInterim(randomBoolean());
        if (randomBoolean()) {
            request.setStart(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setEnd(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setAdvanceTime(randomAlphaOfLengthBetween(1, 20));
        }
        return request;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    public void testNullJobIdThrows() {
        expectThrows(IllegalArgumentException.class, () -> new Request((String) null));
    }
}
