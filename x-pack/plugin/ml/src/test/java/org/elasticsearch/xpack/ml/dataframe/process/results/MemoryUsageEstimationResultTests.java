/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process.results;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class MemoryUsageEstimationResultTests extends AbstractXContentTestCase<MemoryUsageEstimationResult> {

    public static MemoryUsageEstimationResult createRandomResult() {
        return new MemoryUsageEstimationResult(new ByteSizeValue(randomNonNegativeLong()), new ByteSizeValue(randomNonNegativeLong()));
    }

    @Override
    protected MemoryUsageEstimationResult createTestInstance() {
        return createRandomResult();
    }

    @Override
    protected MemoryUsageEstimationResult doParseInstance(XContentParser parser) throws IOException {
        return MemoryUsageEstimationResult.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
