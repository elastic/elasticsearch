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

public class GetCalendarsRequestTests extends AbstractXContentTestCase<GetCalendarsRequest> {

    @Override
    protected GetCalendarsRequest createTestInstance() {
        GetCalendarsRequest request = new GetCalendarsRequest();
        request.setCalendarId(randomAlphaOfLength(9));
        if (randomBoolean()) {
            request.setPageParams(new PageParams(1, 2));
        }
        return request;
    }

    @Override
    protected GetCalendarsRequest doParseInstance(XContentParser parser) {
        return GetCalendarsRequest.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
