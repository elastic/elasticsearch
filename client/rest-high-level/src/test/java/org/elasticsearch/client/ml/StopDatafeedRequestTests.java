/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StopDatafeedRequestTests extends AbstractXContentTestCase<StopDatafeedRequest> {

    public void testCloseAllDatafeedsRequest() {
        StopDatafeedRequest request = StopDatafeedRequest.stopAllDatafeedsRequest();
        assertEquals(request.getDatafeedIds().size(), 1);
        assertEquals(request.getDatafeedIds().get(0), "_all");
    }

    public void testWithNullDatafeedIds() {
        Exception exception = expectThrows(IllegalArgumentException.class, StopDatafeedRequest::new);
        assertEquals(exception.getMessage(), "datafeedIds must not be empty");

        exception = expectThrows(NullPointerException.class, () -> new StopDatafeedRequest("datafeed1", null));
        assertEquals(exception.getMessage(), "datafeedIds must not contain null values");
    }


    @Override
    protected StopDatafeedRequest createTestInstance() {
        int datafeedCount = randomIntBetween(1, 10);
        List<String> datafeedIds = new ArrayList<>(datafeedCount);

        for (int i = 0; i < datafeedCount; i++) {
            datafeedIds.add(randomAlphaOfLength(10));
        }

        StopDatafeedRequest request = new StopDatafeedRequest(datafeedIds.toArray(new String[0]));

        if (randomBoolean()) {
            request.setAllowNoMatch(randomBoolean());
        }

        if (randomBoolean()) {
            request.setTimeout(TimeValue.timeValueMinutes(randomIntBetween(1, 10)));
        }

        if (randomBoolean()) {
            request.setForce(randomBoolean());
        }

        return request;
    }

    @Override
    protected StopDatafeedRequest doParseInstance(XContentParser parser) throws IOException {
        return StopDatafeedRequest.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
