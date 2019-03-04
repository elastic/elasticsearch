/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.dataframe.action.StopDataFrameTransformAction.Request;

public class StopDataFrameTransformActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        TimeValue timeout = randomBoolean() ? TimeValue.timeValueMinutes(randomIntBetween(1, 10)) : null;
        return new Request(randomAlphaOfLengthBetween(1, 10), randomBoolean(), timeout);
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    public void testSameButDifferentTimeout() {
        String id = randomAlphaOfLengthBetween(1, 10);
        boolean waitForCompletion = randomBoolean();

        Request r1 = new Request(id, waitForCompletion, TimeValue.timeValueSeconds(10));
        Request r2 = new Request(id, waitForCompletion, TimeValue.timeValueSeconds(20));

        assertNotEquals(r1,r2);
        assertNotEquals(r1.hashCode(),r2.hashCode());
    }
}
