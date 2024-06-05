/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.explain;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MemoryEstimationTests extends AbstractXContentSerializingTestCase<MemoryEstimation> {

    public static MemoryEstimation createRandom() {
        return new MemoryEstimation(
            randomBoolean() ? ByteSizeValue.ofBytes(randomNonNegativeLong()) : null,
            randomBoolean() ? ByteSizeValue.ofBytes(randomNonNegativeLong()) : null
        );
    }

    @Override
    protected MemoryEstimation createTestInstance() {
        return createRandom();
    }

    @Override
    protected MemoryEstimation mutateInstance(MemoryEstimation instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<MemoryEstimation> instanceReader() {
        return MemoryEstimation::new;
    }

    @Override
    protected MemoryEstimation doParseInstance(XContentParser parser) throws IOException {
        return MemoryEstimation.PARSER.apply(parser, null);
    }

    public void testConstructor_NullValues() {
        MemoryEstimation memoryEstimation = new MemoryEstimation(null, null);
        assertThat(memoryEstimation.getExpectedMemoryWithoutDisk(), nullValue());
        assertThat(memoryEstimation.getExpectedMemoryWithDisk(), nullValue());
    }

    public void testConstructor_SmallValues() {
        MemoryEstimation memoryEstimation = new MemoryEstimation(
            new ByteSizeValue(120, ByteSizeUnit.KB),
            new ByteSizeValue(30, ByteSizeUnit.KB)
        );
        assertThat(memoryEstimation.getExpectedMemoryWithoutDisk(), equalTo(new ByteSizeValue(120, ByteSizeUnit.KB)));
        assertThat(memoryEstimation.getExpectedMemoryWithDisk(), equalTo(new ByteSizeValue(30, ByteSizeUnit.KB)));
    }

    public void testConstructor() {
        MemoryEstimation memoryEstimation = new MemoryEstimation(
            new ByteSizeValue(20, ByteSizeUnit.MB),
            new ByteSizeValue(10, ByteSizeUnit.MB)
        );
        assertThat(memoryEstimation.getExpectedMemoryWithoutDisk(), equalTo(new ByteSizeValue(20, ByteSizeUnit.MB)));
        assertThat(memoryEstimation.getExpectedMemoryWithDisk(), equalTo(new ByteSizeValue(10, ByteSizeUnit.MB)));
    }
}
