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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MemoryUsageEstimationResultTests extends AbstractXContentTestCase<MemoryUsageEstimationResult> {

    public static MemoryUsageEstimationResult createRandomResult() {
        return new MemoryUsageEstimationResult(
            randomBoolean() ? new ByteSizeValue(randomNonNegativeLong()) : null,
            randomBoolean() ? new ByteSizeValue(randomNonNegativeLong()) : null);
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

    public void testConstructor_NullValues() {
        MemoryUsageEstimationResult result = new MemoryUsageEstimationResult(null, null);
        assertThat(result.getExpectedMemoryWithoutDisk(), nullValue());
        assertThat(result.getExpectedMemoryWithDisk(), nullValue());
    }

    public void testConstructor() {
        MemoryUsageEstimationResult result = new MemoryUsageEstimationResult(new ByteSizeValue(2048), new ByteSizeValue(1024));
        assertThat(result.getExpectedMemoryWithoutDisk(), equalTo(new ByteSizeValue(2048)));
        assertThat(result.getExpectedMemoryWithDisk(), equalTo(new ByteSizeValue(1024)));
    }
}
