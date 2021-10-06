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

public class CloseJobRequestTests extends AbstractXContentTestCase<CloseJobRequest> {

    public void testCloseAllJobsRequest() {
        CloseJobRequest request = CloseJobRequest.closeAllJobsRequest();
        assertEquals(request.getJobIds().size(), 1);
        assertEquals(request.getJobIds().get(0), "_all");
    }

    public void testWithNullJobIds() {
        Exception exception = expectThrows(IllegalArgumentException.class, CloseJobRequest::new);
        assertEquals(exception.getMessage(), "jobIds must not be empty");

        exception = expectThrows(NullPointerException.class, () -> new CloseJobRequest("job1", null));
        assertEquals(exception.getMessage(), "jobIds must not contain null values");
    }


    @Override
    protected CloseJobRequest createTestInstance() {
        int jobCount = randomIntBetween(1, 10);
        List<String> jobIds = new ArrayList<>(jobCount);

        for (int i = 0; i < jobCount; i++) {
            jobIds.add(randomAlphaOfLength(10));
        }

        CloseJobRequest request = new CloseJobRequest(jobIds.toArray(new String[0]));

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
    protected CloseJobRequest doParseInstance(XContentParser parser) throws IOException {
        return CloseJobRequest.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
