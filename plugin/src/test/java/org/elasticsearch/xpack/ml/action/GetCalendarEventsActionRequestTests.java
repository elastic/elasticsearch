/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.ml.action.util.PageParams;

public class GetCalendarEventsActionRequestTests extends AbstractStreamableXContentTestCase<GetCalendarEventsAction.Request> {

    @Override
    protected GetCalendarEventsAction.Request createTestInstance() {
        String id = randomAlphaOfLengthBetween(1, 20);
        GetCalendarEventsAction.Request request = new GetCalendarEventsAction.Request(id);
        if (randomBoolean()) {
            request.setAfter(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setBefore(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setPageParams(new PageParams(randomIntBetween(0, 10), randomIntBetween(1, 10)));
        }
        return request;
    }

    @Override
    protected GetCalendarEventsAction.Request createBlankInstance() {
        return new GetCalendarEventsAction.Request();
    }

    @Override
    protected GetCalendarEventsAction.Request doParseInstance(XContentParser parser) {
        return GetCalendarEventsAction.Request.parseRequest(null, parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
