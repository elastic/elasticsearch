/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetDatafeedRequestTests extends AbstractXContentTestCase<GetDatafeedRequest> {

    public void testAllDatafeedRequest() {
        GetDatafeedRequest request = GetDatafeedRequest.getAllDatafeedsRequest();

        assertEquals(request.getDatafeedIds().size(), 1);
        assertEquals(request.getDatafeedIds().get(0), "_all");
    }

    public void testNewWithDatafeedId() {
        Exception exception = expectThrows(NullPointerException.class, () -> new GetDatafeedRequest("feed",null));
        assertEquals(exception.getMessage(), "datafeedIds must not contain null values");
    }

    @Override
    protected GetDatafeedRequest createTestInstance() {
        int count = randomIntBetween(0, 10);
        List<String> datafeedIds = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            datafeedIds.add(DatafeedConfigTests.randomValidDatafeedId());
        }

        GetDatafeedRequest request = new GetDatafeedRequest(datafeedIds);

        if (randomBoolean()) {
            request.setAllowNoMatch(randomBoolean());
        }

        return request;
    }

    @Override
    protected GetDatafeedRequest doParseInstance(XContentParser parser) throws IOException {
        return GetDatafeedRequest.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
