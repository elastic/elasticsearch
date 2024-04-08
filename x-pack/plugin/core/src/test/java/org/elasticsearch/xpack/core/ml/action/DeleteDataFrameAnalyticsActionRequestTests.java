/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.DeleteDataFrameAnalyticsAction.Request;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class DeleteDataFrameAnalyticsActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAlphaOfLength(10));
        request.setForce(randomBoolean());
        if (randomBoolean()) {
            request.timeout(TimeValue.parseTimeValue(randomTimeValue(), "test"));
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

    public void testDefaultTimeout() {
        assertThat(createTestInstance().timeout(), is(notNullValue()));
    }
}
