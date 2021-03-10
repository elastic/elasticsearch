/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.explain;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class MemoryEstimationTests extends AbstractXContentTestCase<MemoryEstimation> {

    public static MemoryEstimation createRandom() {
        return new MemoryEstimation(
            randomBoolean() ? new ByteSizeValue(randomNonNegativeLong()) : null,
            randomBoolean() ? new ByteSizeValue(randomNonNegativeLong()) : null);
    }

    @Override
    protected MemoryEstimation createTestInstance() {
        return createRandom();
    }

    @Override
    protected MemoryEstimation doParseInstance(XContentParser parser) throws IOException {
        return MemoryEstimation.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
