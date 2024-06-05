/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.results;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class AckResultTests extends AbstractXContentTestCase<AckResult> {

    public static AckResult createRandom() {
        return new AckResult(randomBoolean());
    }

    @Override
    protected AckResult createTestInstance() {
        return createRandom();
    }

    @Override
    protected AckResult doParseInstance(XContentParser parser) throws IOException {
        return AckResult.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
