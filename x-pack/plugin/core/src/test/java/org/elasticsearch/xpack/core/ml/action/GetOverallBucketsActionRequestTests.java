/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.GetOverallBucketsAction.Request;

public class GetOverallBucketsActionRequestTests extends AbstractSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAlphaOfLengthBetween(1, 20));

        if (randomBoolean()) {
            request.setTopN(randomIntBetween(1, 1000));
        }
        if (randomBoolean()) {
            request.setBucketSpan(TimeValue.timeValueSeconds(randomIntBetween(1, 1_000_000)));
        }
        if (randomBoolean()) {
            request.setStart(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            request.setExcludeInterim(randomBoolean());
        }
        if (randomBoolean()) {
            request.setOverallScore(randomDouble());
        }
        if (randomBoolean()) {
            request.setEnd(randomNonNegativeLong());
        }
        request.setAllowNoMatch(randomBoolean());
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
        return Request.parseRequest(null, parser);
    }

}
