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
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.MlFilterTests;

import java.io.IOException;

public class PutFilterActionResponseTests extends AbstractSerializingTestCase<PutFilterAction.Response> {

    @Override
    protected PutFilterAction.Response createTestInstance() {
        return new PutFilterAction.Response(MlFilterTests.createRandom());
    }

    @Override
    protected Writeable.Reader<PutFilterAction.Response> instanceReader() {
        return PutFilterAction.Response::new;
    }

    @Override
    protected PutFilterAction.Response doParseInstance(XContentParser parser) throws IOException {
        return new PutFilterAction.Response(MlFilter.LENIENT_PARSER.parse(parser, null).build());
    }
}
