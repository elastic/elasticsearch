/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.results.OverallBucket;
import org.elasticsearch.client.ml.job.results.OverallBucketTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetOverallBucketsResponseTests extends AbstractXContentTestCase<GetOverallBucketsResponse> {

    @Override
    protected GetOverallBucketsResponse createTestInstance() {
        int listSize = randomInt(10);
        List<OverallBucket> overallBuckets = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            OverallBucket overallBucket = OverallBucketTests.createRandom();
            overallBuckets.add(overallBucket);
        }
        return new GetOverallBucketsResponse(overallBuckets, listSize);
    }

    @Override
    protected GetOverallBucketsResponse doParseInstance(XContentParser parser) throws IOException {
        return GetOverallBucketsResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
