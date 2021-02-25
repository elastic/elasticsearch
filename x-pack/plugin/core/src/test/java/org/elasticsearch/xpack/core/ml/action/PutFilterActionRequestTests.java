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
import org.elasticsearch.xpack.core.ml.action.PutFilterAction.Request;
import org.elasticsearch.xpack.core.ml.job.config.MlFilterTests;

public class PutFilterActionRequestTests extends AbstractSerializingTestCase<Request> {

    private final String filterId = MlFilterTests.randomValidFilterId();

    @Override
    protected Request createTestInstance() {
        return new PutFilterAction.Request(MlFilterTests.createRandom(filterId));
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
        return PutFilterAction.Request.parseRequest(filterId, parser);
    }
}
