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

public class GetJobRequestTests extends AbstractXContentTestCase<GetJobRequest> {

    public void testAllJobsRequest() {
        GetJobRequest request = GetJobRequest.getAllJobsRequest();

        assertEquals(request.getJobIds().size(), 1);
        assertEquals(request.getJobIds().get(0), "_all");
    }

    public void testNewWithJobId() {
        Exception exception = expectThrows(NullPointerException.class, () -> new GetJobRequest("job",null));
        assertEquals(exception.getMessage(), "jobIds must not contain null values");
    }

    @Override
    protected GetJobRequest createTestInstance() {
        int jobCount = randomIntBetween(0, 10);
        List<String> jobIds = new ArrayList<>(jobCount);

        for (int i = 0; i < jobCount; i++) {
            jobIds.add(randomAlphaOfLength(10));
        }

        GetJobRequest request = new GetJobRequest(jobIds);

        if (randomBoolean()) {
            request.setAllowNoMatch(randomBoolean());
        }

        return request;
    }

    @Override
    protected GetJobRequest doParseInstance(XContentParser parser) throws IOException {
        return GetJobRequest.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
