/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class StartDataFrameAnalyticsResponseTests extends AbstractXContentTestCase<StartDataFrameAnalyticsResponse> {

    @Override
    protected StartDataFrameAnalyticsResponse createTestInstance() {
        String node = randomFrom("", randomAlphaOfLength(10), null);
        return new StartDataFrameAnalyticsResponse(randomBoolean(), node);
    }

    @Override
    protected StartDataFrameAnalyticsResponse doParseInstance(XContentParser parser) throws IOException {
        return StartDataFrameAnalyticsResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
