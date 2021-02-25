/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction.Request;

public class GetBucketActionRequestTests extends AbstractSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        GetBucketsAction.Request request = new GetBucketsAction.Request(randomAlphaOfLengthBetween(1, 20));

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
            request.setDescending(randomBoolean());
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
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return GetBucketsAction.Request.parseRequest(null, parser);
    }

}
