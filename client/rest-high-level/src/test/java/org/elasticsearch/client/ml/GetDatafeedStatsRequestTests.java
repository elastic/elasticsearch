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
import java.util.ArrayList;
import java.util.List;

public class GetDatafeedStatsRequestTests extends AbstractXContentTestCase<GetDatafeedStatsRequest> {

    public void testAllDatafeedsRequest() {
        GetDatafeedStatsRequest request = GetDatafeedStatsRequest.getAllDatafeedStatsRequest();

        assertEquals(request.getDatafeedIds().size(), 1);
        assertEquals(request.getDatafeedIds().get(0), "_all");
    }

    public void testNewWithDatafeedId() {
        Exception exception = expectThrows(NullPointerException.class, () -> new GetDatafeedStatsRequest("datafeed", null));
        assertEquals(exception.getMessage(), "datafeedIds must not contain null values");
    }

    @Override
    protected GetDatafeedStatsRequest createTestInstance() {
        int datafeedCount = randomIntBetween(0, 10);
        List<String> datafeedIds = new ArrayList<>(datafeedCount);

        for (int i = 0; i < datafeedCount; i++) {
            datafeedIds.add(randomAlphaOfLength(10));
        }

        GetDatafeedStatsRequest request = new GetDatafeedStatsRequest(datafeedIds);

        if (randomBoolean()) {
            request.setAllowNoMatch(randomBoolean());
        }

        return request;
    }

    @Override
    protected GetDatafeedStatsRequest doParseInstance(XContentParser parser) throws IOException {
        return GetDatafeedStatsRequest.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
