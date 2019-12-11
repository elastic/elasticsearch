/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetInfluencersAction.Request;

public class GetInfluencersActionRequestTests extends AbstractSerializingTestCase<Request> {

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return GetInfluencersAction.Request.parseRequest(null, parser);
    }

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            String start = randomBoolean() ? randomAlphaOfLengthBetween(1, 20) : String.valueOf(randomNonNegativeLong());
            request.setStart(start);
        }
        if (randomBoolean()) {
            String end = randomBoolean() ? randomAlphaOfLengthBetween(1, 20) : String.valueOf(randomNonNegativeLong());
            request.setEnd(end);
        }
        if (randomBoolean()) {
            request.setInfluencerScore(randomDouble());
        }
        if (randomBoolean()) {
            request.setExcludeInterim(randomBoolean());
        }
        if (randomBoolean()) {
            request.setSort(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setDescending(randomBoolean());
        }
        if (randomBoolean()) {
            int from = randomInt(10000);
            int size = randomInt(10000);
            request.setPageParams(new PageParams(from, size));
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
}
