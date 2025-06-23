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
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.MlFilterTests;

import java.io.IOException;

public class PutFilterActionResponseTests extends AbstractXContentSerializingTestCase<PutFilterAction.Response> {

    @Override
    protected PutFilterAction.Response createTestInstance() {
        return new PutFilterAction.Response(MlFilterTests.createRandom());
    }

    @Override
    protected PutFilterAction.Response mutateInstance(PutFilterAction.Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
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
