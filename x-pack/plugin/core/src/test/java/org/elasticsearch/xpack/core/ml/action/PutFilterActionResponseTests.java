/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.MlFilterTests;

import java.io.IOException;

public class PutFilterActionResponseTests extends AbstractStreamableXContentTestCase<PutFilterAction.Response> {

    @Override
    protected PutFilterAction.Response createTestInstance() {
        return new PutFilterAction.Response(MlFilterTests.createRandom());
    }

    @Override
    protected PutFilterAction.Response createBlankInstance() {
        return new PutFilterAction.Response();
    }

    @Override
    protected PutFilterAction.Response doParseInstance(XContentParser parser) throws IOException {
        return new PutFilterAction.Response(MlFilter.LENIENT_PARSER.parse(parser, null).build());
    }
}
