/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.elasticsearch.test.ESTestCase;

public class StaticPipelineConfigResolverTests extends ESTestCase {

    private static final String TIMESTAMP_FIELD_NAME = "@timestamp";

    public void testResolvesBaselinePipelineShapeForNonTimestampField() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final PipelineConfig config = resolver.resolve(new FieldContext(randomBlockSize(), randomNonTimestampFieldName()));

        assertEquals("delta>offset>gcd>bitPack", config.describeStages());
        assertEquals(PipelineDescriptor.DataType.LONG, config.dataType());
        assertEquals(3, config.transforms().size());
        assertNotNull(config.payload());
    }

    public void testResolvesTimestampPipelineShape() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final PipelineConfig config = resolver.resolve(new FieldContext(randomBlockSize(), TIMESTAMP_FIELD_NAME));

        assertEquals("splitDelta>delta>offset>gcd>bitPack", config.describeStages());
        assertEquals(PipelineDescriptor.DataType.LONG, config.dataType());
        assertEquals(4, config.transforms().size());
        assertNotNull(config.payload());
    }

    public void testPropagatesBlockSizeFromContext() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = randomBlockSize();

        final PipelineConfig config = resolver.resolve(new FieldContext(blockSize, randomNonTimestampFieldName()));

        assertEquals(blockSize, config.blockSize());
    }

    public void testTimestampFieldGetsDifferentPipelineThanOtherFields() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = randomBlockSize();

        final PipelineConfig timestamp = resolver.resolve(new FieldContext(blockSize, TIMESTAMP_FIELD_NAME));
        final PipelineConfig other = resolver.resolve(new FieldContext(blockSize, "metric.value"));

        assertNotEquals(timestamp, other);
        assertEquals("splitDelta>delta>offset>gcd>bitPack", timestamp.describeStages());
        assertEquals("delta>offset>gcd>bitPack", other.describeStages());
    }

    public void testRepeatedResolveReturnsEqualConfigs() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final FieldContext context = new FieldContext(randomBlockSize(), randomNonTimestampFieldName());

        assertEquals(resolver.resolve(context), resolver.resolve(context));
    }

    public void testCachedBlockSizesReturnSameInstanceForNonTimestamp() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = randomFrom(128, 512);

        final PipelineConfig first = resolver.resolve(new FieldContext(blockSize, randomNonTimestampFieldName()));
        final PipelineConfig second = resolver.resolve(new FieldContext(blockSize, randomNonTimestampFieldName()));
        assertSame(first, second);
    }

    public void testCachedBlockSizesReturnSameInstanceForTimestamp() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = randomFrom(128, 512);

        final PipelineConfig first = resolver.resolve(new FieldContext(blockSize, TIMESTAMP_FIELD_NAME));
        final PipelineConfig second = resolver.resolve(new FieldContext(blockSize, TIMESTAMP_FIELD_NAME));
        assertSame(first, second);
    }

    public void testUncachedBlockSizeStillResolves() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = 1 << randomIntBetween(4, 6);

        final PipelineConfig nonTimestamp = resolver.resolve(new FieldContext(blockSize, randomNonTimestampFieldName()));
        assertEquals(blockSize, nonTimestamp.blockSize());
        assertEquals("delta>offset>gcd>bitPack", nonTimestamp.describeStages());

        final PipelineConfig timestamp = resolver.resolve(new FieldContext(blockSize, TIMESTAMP_FIELD_NAME));
        assertEquals(blockSize, timestamp.blockSize());
        assertEquals("splitDelta>delta>offset>gcd>bitPack", timestamp.describeStages());
    }

    private static int randomBlockSize() {
        return 1 << randomIntBetween(4, 10);
    }

    private static String randomNonTimestampFieldName() {
        // NOTE: randomAlphaOfLengthBetween only produces ASCII alpha characters, so the
        // result can never equal TIMESTAMP_FIELD_NAME ("@timestamp" starts with @).
        return randomAlphaOfLengthBetween(1, 16);
    }
}
