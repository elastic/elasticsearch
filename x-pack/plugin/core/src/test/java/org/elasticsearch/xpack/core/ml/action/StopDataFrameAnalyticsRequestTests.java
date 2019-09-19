/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.StopDataFrameAnalyticsAction.Request;

import java.util.HashSet;
import java.util.Set;

public class StopDataFrameAnalyticsRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAlphaOfLength(20));
        if (randomBoolean()) {
            request.setTimeout(TimeValue.timeValueMillis(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setAllowNoMatch(randomBoolean());
        }
        if (randomBoolean()) {
            request.setForce(randomBoolean());
        }
        int expandedIdsCount = randomIntBetween(0, 10);
        Set<String> expandedIds = new HashSet<>();
        for (int i = 0; i < expandedIdsCount; i++) {
            expandedIds.add(randomAlphaOfLength(20));
        }
        request.setExpandedIds(expandedIds);
        return request;
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }
}
