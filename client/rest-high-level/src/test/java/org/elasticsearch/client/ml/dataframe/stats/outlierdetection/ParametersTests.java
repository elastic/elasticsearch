/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.stats.outlierdetection;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class ParametersTests extends AbstractXContentTestCase<Parameters> {

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Parameters doParseInstance(XContentParser parser) throws IOException {
        return Parameters.PARSER.apply(parser, null);
    }

    @Override
    protected Parameters createTestInstance() {
        return createRandom();
    }

    public static Parameters createRandom() {
        return new Parameters(
            randomBoolean() ? null : randomIntBetween(1, Integer.MAX_VALUE),
            randomBoolean() ? null : randomAlphaOfLength(5),
            randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomBoolean()
        );
    }
}
