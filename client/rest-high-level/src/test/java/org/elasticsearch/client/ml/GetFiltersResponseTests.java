/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.config.MlFilter;
import org.elasticsearch.client.ml.job.config.MlFilterTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetFiltersResponseTests extends AbstractXContentTestCase<GetFiltersResponse> {

    @Override
    protected GetFiltersResponse createTestInstance() {
        int count = randomIntBetween(1, 5);
        List<MlFilter.Builder> results = new ArrayList<>(count);
        for(int i = 0; i < count; i++) {
            results.add(MlFilterTests.createRandomBuilder(randomAlphaOfLength(10)));
        }

        return new GetFiltersResponse(results, count);
    }

    @Override
    protected GetFiltersResponse doParseInstance(XContentParser parser) throws IOException {
        return GetFiltersResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
