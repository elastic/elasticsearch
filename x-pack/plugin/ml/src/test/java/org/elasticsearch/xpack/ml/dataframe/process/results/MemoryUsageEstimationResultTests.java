/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
            randomBoolean() ? ByteSizeValue.ofBytes(randomNonNegativeLong()) : null,
            randomBoolean() ? ByteSizeValue.ofBytes(randomNonNegativeLong()) : null);
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

    public void testConstructor_SmallValues() {
        MemoryUsageEstimationResult result =
            new MemoryUsageEstimationResult(ByteSizeValue.ofKb(120), ByteSizeValue.ofKb(30));
        assertThat(result.getExpectedMemoryWithoutDisk(), equalTo(ByteSizeValue.ofKb(120)));
        assertThat(result.getExpectedMemoryWithDisk(), equalTo(ByteSizeValue.ofKb(30)));
    }

    public void testConstructor() {
        MemoryUsageEstimationResult result =
            new MemoryUsageEstimationResult(ByteSizeValue.ofMb(20), ByteSizeValue.ofMb(10));
        assertThat(result.getExpectedMemoryWithoutDisk(), equalTo(ByteSizeValue.ofMb(20)));
        assertThat(result.getExpectedMemoryWithDisk(), equalTo(ByteSizeValue.ofMb(10)));
    }
}
