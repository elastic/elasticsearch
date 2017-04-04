/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.action.GetBucketsAction.Request;
import org.elasticsearch.xpack.ml.action.util.PageParams;
import org.elasticsearch.xpack.ml.support.AbstractStreamableXContentTestCase;

public class GetBucketActionRequestTests extends AbstractStreamableXContentTestCase<GetBucketsAction.Request> {

    @Override
    protected Request createTestInstance() {
        GetBucketsAction.Request request = new GetBucketsAction.Request(randomAlphaOfLengthBetween(1, 20));

        if (randomBoolean()) {
            request.setTimestamp(String.valueOf(randomLong()));
        } else {
            if (randomBoolean()) {
                request.setPartitionValue(randomAlphaOfLengthBetween(1, 20));
            }
            if (randomBoolean()) {
                request.setStart(String.valueOf(randomLong()));
            }
            if (randomBoolean()) {
                request.setEnd(String.valueOf(randomLong()));
            }
            if (randomBoolean()) {
                request.setIncludeInterim(randomBoolean());
            }
            if (randomBoolean()) {
                request.setAnomalyScore(randomDouble());
            }
            if (randomBoolean()) {
                request.setPartitionValue(randomAlphaOfLengthBetween(1, 20));
            }
            if (randomBoolean()) {
                int from = randomInt(PageParams.MAX_FROM_SIZE_SUM);
                int maxSize = PageParams.MAX_FROM_SIZE_SUM - from;
                int size = randomInt(maxSize);
                request.setPageParams(new PageParams(from, size));
            }
        }
        if (randomBoolean()) {
            request.setExpand(randomBoolean());
        }
        if (randomBoolean()) {
            request.setIncludeInterim(randomBoolean());
        }
        return request;
    }

    @Override
    protected Request createBlankInstance() {
        return new GetBucketsAction.Request();
    }

    @Override
    protected Request parseInstance(XContentParser parser) {
        return GetBucketsAction.Request.parseRequest(null, parser);
    }

}
