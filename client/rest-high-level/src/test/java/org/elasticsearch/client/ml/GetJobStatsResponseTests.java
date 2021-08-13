/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.client.ml.job.stats.JobStats;
import org.elasticsearch.client.ml.job.stats.JobStatsTests;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetJobStatsResponseTests extends AbstractXContentTestCase<GetJobStatsResponse> {

    @Override
    protected GetJobStatsResponse createTestInstance() {

        int count = randomIntBetween(1, 5);
        List<JobStats> results = new ArrayList<>(count);
        for(int i = 0; i < count; i++) {
            results.add(JobStatsTests.createRandomInstance());
        }

        return new GetJobStatsResponse(results, count);
    }

    @Override
    protected GetJobStatsResponse doParseInstance(XContentParser parser) throws IOException {
        return GetJobStatsResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
