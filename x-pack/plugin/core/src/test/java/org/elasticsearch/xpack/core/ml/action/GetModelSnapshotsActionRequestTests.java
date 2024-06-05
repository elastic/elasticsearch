/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction.Request;

public class GetModelSnapshotsActionRequestTests extends AbstractXContentSerializingTestCase<Request> {

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return Request.parseRequest(null, null, parser);
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
    protected Request mutateInstance(Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

}
