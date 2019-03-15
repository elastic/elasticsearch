/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction.Request;
import org.elasticsearch.xpack.core.action.util.PageParams;

public class GetModelSnapshotsActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return GetModelSnapshotsAction.Request.parseRequest(null, null, parser);
    }

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAlphaOfLengthBetween(1, 20), randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            request.setStart(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setEnd(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setSort(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setDescOrder(randomBoolean());
        }
        if (randomBoolean()) {
            int from = randomInt(10000);
            int size = randomInt(10000);
            request.setPageParams(new PageParams(from, size));
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

}
