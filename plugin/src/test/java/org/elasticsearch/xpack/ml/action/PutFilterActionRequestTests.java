/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.ml.action.PutFilterAction.Request;
import org.elasticsearch.xpack.ml.job.config.MlFilter;

import java.util.ArrayList;
import java.util.List;

public class PutFilterActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    private final String filterId = randomAlphaOfLengthBetween(1, 20);

    @Override
    protected Request createTestInstance() {
        int size = randomInt(10);
        List<String> items = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            items.add(randomAlphaOfLengthBetween(1, 20));
        }
        MlFilter filter = new MlFilter(filterId, items);
        return new PutFilterAction.Request(filter);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Request createBlankInstance() {
        return new PutFilterAction.Request();
    }

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return PutFilterAction.Request.parseRequest(filterId, parser);
    }

}
