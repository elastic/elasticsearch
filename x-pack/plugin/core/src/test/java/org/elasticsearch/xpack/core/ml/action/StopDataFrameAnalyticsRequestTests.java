/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.StopDataFrameAnalyticsAction.Request;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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

    public void testDefaultTimeout() throws IOException {
        {
            Request request = new Request("foo");
            assertThat(request.getTimeout(), is(notNullValue()));
        }
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{}")) {
            Request request = Request.parseRequest("foo", parser);
            assertThat(request.getTimeout(), is(notNullValue()));
        }
    }
}
