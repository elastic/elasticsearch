/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class StartDatafeedRequestTests extends AbstractXContentTestCase<StartDatafeedRequest> {

    public static StartDatafeedRequest createRandomInstance(String datafeedId) {
        StartDatafeedRequest request = new StartDatafeedRequest(datafeedId);

        if (randomBoolean()) {
            request.setStart(String.valueOf(randomLongBetween(1, 1000)));
        }
        if (randomBoolean()) {
            request.setEnd(String.valueOf(randomLongBetween(1, 1000)));
        }
        if (randomBoolean()) {
            request.setTimeout(TimeValue.timeValueMinutes(randomLongBetween(1, 1000)));
        }

        return request;
    }

    @Override
    protected StartDatafeedRequest createTestInstance() {
        String datafeedId = DatafeedConfigTests.randomValidDatafeedId();
        return createRandomInstance(datafeedId);
    }

    @Override
    protected StartDatafeedRequest doParseInstance(XContentParser parser) throws IOException {
        return StartDatafeedRequest.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
