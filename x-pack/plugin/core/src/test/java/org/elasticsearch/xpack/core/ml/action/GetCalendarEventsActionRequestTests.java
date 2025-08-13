/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.action.util.PageParams;

public class GetCalendarEventsActionRequestTests extends AbstractXContentSerializingTestCase<GetCalendarEventsAction.Request> {

    @Override
    protected GetCalendarEventsAction.Request createTestInstance() {
        String id = randomAlphaOfLengthBetween(1, 20);
        GetCalendarEventsAction.Request request = new GetCalendarEventsAction.Request(id);
        if (randomBoolean()) {
            request.setStart(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setEnd(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setJobId(randomAlphaOfLength(8));
        }
        if (randomBoolean()) {
            request.setPageParams(new PageParams(randomIntBetween(0, 10), randomIntBetween(1, 10)));
        }
        return request;
    }

    @Override
    protected GetCalendarEventsAction.Request mutateInstance(GetCalendarEventsAction.Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<GetCalendarEventsAction.Request> instanceReader() {
        return GetCalendarEventsAction.Request::new;
    }

    @Override
    protected GetCalendarEventsAction.Request doParseInstance(XContentParser parser) {
        return GetCalendarEventsAction.Request.parseRequest(null, parser);
    }

    public void testValidate() {
        GetCalendarEventsAction.Request request = new GetCalendarEventsAction.Request("cal-name");
        request.setJobId("foo");

        ActionRequestValidationException validationException = request.validate();
        assertNotNull(validationException);
        assertEquals("Validation Failed: 1: If job_id is used calendar_id must be '_all' or '*';", validationException.getMessage());

        request = new GetCalendarEventsAction.Request("_all");
        request.setJobId("foo");
        assertNull(request.validate());
    }
}
