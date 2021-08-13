/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class GetBucketsRequestTests extends AbstractXContentTestCase<GetBucketsRequest> {

    @Override
    protected GetBucketsRequest createTestInstance() {
        GetBucketsRequest request = new GetBucketsRequest(randomAlphaOfLengthBetween(1, 20));

        if (randomBoolean()) {
            request.setTimestamp(String.valueOf(randomLong()));
        } else {
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
                request.setAnomalyScore(randomDouble());
            }
            if (randomBoolean()) {
                int from = randomInt(10000);
                int size = randomInt(10000);
                request.setPageParams(new PageParams(from, size));
            }
            if (randomBoolean()) {
                request.setSort("anomaly_score");
            }
            if (randomBoolean()) {
                request.setDescending(randomBoolean());
            }
        }
        if (randomBoolean()) {
            request.setExpand(randomBoolean());
        }
        if (randomBoolean()) {
            request.setExcludeInterim(randomBoolean());
        }
        return request;
    }

    @Override
    protected GetBucketsRequest doParseInstance(XContentParser parser) throws IOException {
        return GetBucketsRequest.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
