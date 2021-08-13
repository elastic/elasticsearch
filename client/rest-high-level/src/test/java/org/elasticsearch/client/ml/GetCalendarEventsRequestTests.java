/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

public class GetCalendarEventsRequestTests extends AbstractXContentTestCase<GetCalendarEventsRequest> {

    @Override
    protected GetCalendarEventsRequest createTestInstance() {
        String calendarId = randomAlphaOfLengthBetween(1, 10);
        GetCalendarEventsRequest request = new GetCalendarEventsRequest(calendarId);
        if (randomBoolean()) {
            request.setPageParams(new PageParams(1, 2));
        }
        if (randomBoolean()) {
            request.setEnd(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            request.setStart(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            request.setJobId(randomAlphaOfLength(10));
        }
        return request;
    }

    @Override
    protected GetCalendarEventsRequest doParseInstance(XContentParser parser) {
        return GetCalendarEventsRequest.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
