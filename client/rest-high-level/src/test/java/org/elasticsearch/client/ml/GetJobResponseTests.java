/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.config.JobTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class GetJobResponseTests extends AbstractXContentTestCase<GetJobResponse> {

    @Override
    protected GetJobResponse createTestInstance() {

        int count = randomIntBetween(1, 5);
        List<Job.Builder> results = new ArrayList<>(count);
        for(int i = 0; i < count; i++) {
            results.add(JobTests.createRandomizedJobBuilder());
        }

        return new GetJobResponse(results, count);
    }

    @Override
    protected GetJobResponse doParseInstance(XContentParser parser) throws IOException {
        return GetJobResponse.fromXContent(parser);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
