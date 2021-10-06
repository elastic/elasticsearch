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

public class GetOverallBucketsRequestTests extends AbstractXContentTestCase<GetOverallBucketsRequest> {

    @Override
    protected GetOverallBucketsRequest createTestInstance() {
        GetOverallBucketsRequest request = new GetOverallBucketsRequest(randomAlphaOfLengthBetween(1, 20));

        if (randomBoolean()) {
            request.setTopN(randomIntBetween(1, 10));
        }

        if (randomBoolean()) {
            request.setBucketSpan(TimeValue.timeValueSeconds(randomIntBetween(1, 1_000_000)));
        }
        if (randomBoolean()) {
            request.setStart(String.valueOf(randomLong()));
        }
        if (randomBoolean()) {
            request.setEnd(String.valueOf(randomLong()));
        }
        if (randomBoolean()) {
            request.setExcludeInterim(randomBoolean());
        }
        if (randomBoolean()) {
            request.setOverallScore(randomDouble());
        }
        if (randomBoolean()) {
            request.setExcludeInterim(randomBoolean());
        }
        return request;
    }

    @Override
    protected GetOverallBucketsRequest doParseInstance(XContentParser parser) throws IOException {
        return GetOverallBucketsRequest.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
