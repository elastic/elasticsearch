/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.process;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class ControllerResponseTests extends AbstractXContentTestCase<ControllerResponse> {

    @Override
    protected ControllerResponse createTestInstance() {
        return new ControllerResponse(randomIntBetween(1, 1000000), randomBoolean(), randomBoolean() ? null : randomAlphaOfLength(100));
    }

    @Override
    protected ControllerResponse doParseInstance(XContentParser parser) throws IOException {
        return ControllerResponse.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
