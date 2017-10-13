/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.ml.action.GetOverallBucketsAction.Request;

public class GetOverallBucketsActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAlphaOfLengthBetween(1, 20));

        if (randomBoolean()) {
            request.setTopN(randomIntBetween(1, 1000));
        }
        request.setAllowNoJobs(randomBoolean());
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
        return request;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return Request.parseRequest(null, parser);
    }

}
