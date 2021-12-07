/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.action.util.PageParams;

public class GetCalendarsActionRequestTests extends AbstractSerializingTestCase<GetCalendarsAction.Request> {

    @Override
    protected GetCalendarsAction.Request createTestInstance() {
        GetCalendarsAction.Request request = new GetCalendarsAction.Request();
        if (randomBoolean()) {
            request.setCalendarId(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setPageParams(PageParams.defaultParams());
        }
        return request;
    }

    @Override
    protected Writeable.Reader<GetCalendarsAction.Request> instanceReader() {
        return GetCalendarsAction.Request::new;
    }

    @Override
    protected GetCalendarsAction.Request doParseInstance(XContentParser parser) {
        return GetCalendarsAction.Request.parseRequest(null, parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
