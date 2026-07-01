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
        final PipelineConfig config = resolver.resolve(
            new FieldContext(randomBlockSize(), randomNonTimestampFieldName(), null, null, null, false)
        );

        assertEquals("delta>offset>gcd>bitPack", config.describeStages());
        assertEquals(PipelineDescriptor.DataType.LONG, config.dataType());
        assertEquals(3, config.transforms().size());
        assertNotNull(config.payload());
    }

    public void testResolvesTimestampPipelineShape() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final PipelineConfig config = resolver.resolve(new FieldContext(randomBlockSize(), TIMESTAMP_FIELD_NAME, null, null, null, false));

        assertEquals("splitDelta>delta>offset>gcd>bitPack", config.describeStages());
        assertEquals(PipelineDescriptor.DataType.LONG, config.dataType());
        assertEquals(4, config.transforms().size());
        assertNotNull(config.payload());
    }

    public void testPropagatesBlockSizeFromContext() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = randomBlockSize();

        final PipelineConfig config = resolver.resolve(new FieldContext(blockSize, randomNonTimestampFieldName(), null, null, null, false));

        assertEquals(blockSize, config.blockSize());
    }

    public void testTimestampFieldGetsDifferentPipelineThanOtherFields() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = randomBlockSize();

        final PipelineConfig timestamp = resolver.resolve(new FieldContext(blockSize, TIMESTAMP_FIELD_NAME, null, null, null, false));
        final PipelineConfig other = resolver.resolve(new FieldContext(blockSize, "metric.value", null, null, null, false));

        assertNotEquals(timestamp, other);
        assertEquals("splitDelta>delta>offset>gcd>bitPack", timestamp.describeStages());
        assertEquals("delta>offset>gcd>bitPack", other.describeStages());
    }

    public void testRepeatedResolveReturnsEqualConfigs() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final FieldContext context = new FieldContext(randomBlockSize(), randomNonTimestampFieldName(), null, null, null, false);

        assertEquals(resolver.resolve(context), resolver.resolve(context));
    }

    public void testCachedBlockSizesReturnSameInstanceForNonTimestamp() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = randomFrom(128, 512);

        final PipelineConfig first = resolver.resolve(new FieldContext(blockSize, randomNonTimestampFieldName(), null, null, null, false));
        final PipelineConfig second = resolver.resolve(new FieldContext(blockSize, randomNonTimestampFieldName(), null, null, null, false));
        assertSame(first, second);
    }

    public void testCachedBlockSizesReturnSameInstanceForTimestamp() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = randomFrom(128, 512);

        final PipelineConfig first = resolver.resolve(new FieldContext(blockSize, TIMESTAMP_FIELD_NAME, null, null, null, false));
        final PipelineConfig second = resolver.resolve(new FieldContext(blockSize, TIMESTAMP_FIELD_NAME, null, null, null, false));
        assertSame(first, second);
    }

    public void testUncachedBlockSizeStillResolves() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = 1 << randomIntBetween(4, 6);

        final PipelineConfig nonTimestamp = resolver.resolve(
            new FieldContext(blockSize, randomNonTimestampFieldName(), null, null, null, false)
        );
        assertEquals(blockSize, nonTimestamp.blockSize());
        assertEquals("delta>offset>gcd>bitPack", nonTimestamp.describeStages());

        final PipelineConfig timestamp = resolver.resolve(new FieldContext(blockSize, TIMESTAMP_FIELD_NAME, null, null, null, false));
        assertEquals(blockSize, timestamp.blockSize());
        assertEquals("splitDelta>delta>offset>gcd>bitPack", timestamp.describeStages());
    }

    public void testResolvesSplitDeltaPipelineForLongCounter() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final FieldContext context = new FieldContext(
            randomBlockSize(),
            randomNonTimestampFieldName(),
            PipelineDescriptor.DataType.LONG,
            MetricRole.COUNTER,
            null,
            false
        );

        final PipelineConfig config = resolver.resolve(context);

        assertEquals("splitDelta>delta>offset>gcd>bitPack", config.describeStages());
        assertEquals(PipelineDescriptor.DataType.LONG, config.dataType());
    }

    public void testResolvesBaselinePipelineForLongGauge() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final FieldContext context = new FieldContext(
            randomBlockSize(),
            randomNonTimestampFieldName(),
            PipelineDescriptor.DataType.LONG,
            MetricRole.GAUGE,
            null,
            false
        );

        final PipelineConfig config = resolver.resolve(context);

        assertEquals("delta>offset>gcd>bitPack", config.describeStages());
    }

    public void testResolvesAlpDoubleCounterPipelineForDoubleCounter() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final FieldContext context = new FieldContext(
            randomBlockSize(),
            randomNonTimestampFieldName(),
            PipelineDescriptor.DataType.DOUBLE,
            MetricRole.COUNTER,
            null,
            false
        );

        final PipelineConfig config = resolver.resolve(context);

        assertEquals("alpDouble>splitDelta>delta>offset>gcd>bitPack", config.describeStages());
        assertEquals(PipelineDescriptor.DataType.DOUBLE, config.dataType());
        assertEquals(5, config.transforms().size());
    }

    public void testResolvesAlpDoublePipelineForDoubleGauge() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final FieldContext context = new FieldContext(
            randomBlockSize(),
            randomNonTimestampFieldName(),
            PipelineDescriptor.DataType.DOUBLE,
            MetricRole.GAUGE,
            null,
            false
        );

        final PipelineConfig config = resolver.resolve(context);

        assertEquals("alpDouble>delta>offset>gcd>bitPack", config.describeStages());
        assertEquals(PipelineDescriptor.DataType.DOUBLE, config.dataType());
        assertEquals(4, config.transforms().size());
        assertNotNull(config.payload());
    }

    public void testResolvesBaselinePipelineForDoubleWithoutMetricType() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final FieldContext context = new FieldContext(
            randomBlockSize(),
            randomNonTimestampFieldName(),
            PipelineDescriptor.DataType.DOUBLE,
            null,
            null,
            false
        );

        final PipelineConfig config = resolver.resolve(context);

        assertEquals("delta>offset>gcd>bitPack", config.describeStages());
    }

    public void testCachedBlockSizesReturnSameInstanceForDoubleGauge() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = randomFrom(128, 512);

        final PipelineConfig first = resolver.resolve(
            new FieldContext(blockSize, randomNonTimestampFieldName(), PipelineDescriptor.DataType.DOUBLE, MetricRole.GAUGE, null, false)
        );
        final PipelineConfig second = resolver.resolve(
            new FieldContext(blockSize, randomNonTimestampFieldName(), PipelineDescriptor.DataType.DOUBLE, MetricRole.GAUGE, null, false)
        );
        assertSame(first, second);
    }

    public void testDoubleGaugePipelineDiffersFromBaselineAndSplitDelta() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = randomBlockSize();
        final String fieldName = randomNonTimestampFieldName();

        final PipelineConfig alpDouble = resolver.resolve(
            new FieldContext(blockSize, fieldName, PipelineDescriptor.DataType.DOUBLE, MetricRole.GAUGE, null, false)
        );
        final PipelineConfig baseline = resolver.resolve(
            new FieldContext(blockSize, fieldName, PipelineDescriptor.DataType.LONG, MetricRole.GAUGE, null, false)
        );
        final PipelineConfig splitDelta = resolver.resolve(new FieldContext(blockSize, TIMESTAMP_FIELD_NAME, null, null, null, false));

        assertNotEquals(alpDouble, baseline);
        assertNotEquals(alpDouble, splitDelta);
        assertEquals("alpDouble>delta>offset>gcd>bitPack", alpDouble.describeStages());
        assertEquals("delta>offset>gcd>bitPack", baseline.describeStages());
        assertEquals("splitDelta>delta>offset>gcd>bitPack", splitDelta.describeStages());
    }

    public void testUncachedBlockSizeStillResolvesForDoubleGauge() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = 1 << randomIntBetween(4, 6);

        final PipelineConfig config = resolver.resolve(
            new FieldContext(blockSize, randomNonTimestampFieldName(), PipelineDescriptor.DataType.DOUBLE, MetricRole.GAUGE, null, false)
        );

        assertEquals(blockSize, config.blockSize());
        assertEquals("alpDouble>delta>offset>gcd>bitPack", config.describeStages());
    }

    public void testResolvesBaselinePipelineForLongWithoutMetricType() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final FieldContext context = new FieldContext(
            randomBlockSize(),
            randomNonTimestampFieldName(),
            PipelineDescriptor.DataType.LONG,
            null,
            null,
            false
        );

        final PipelineConfig config = resolver.resolve(context);

        assertEquals("delta>offset>gcd>bitPack", config.describeStages());
    }

    public void testCounterAndTimestampShareCachedConfigForKnownBlockSizes() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = randomFrom(128, 512);

        final PipelineConfig counter = resolver.resolve(
            new FieldContext(blockSize, randomNonTimestampFieldName(), PipelineDescriptor.DataType.LONG, MetricRole.COUNTER, null, false)
        );
        final PipelineConfig timestamp = resolver.resolve(new FieldContext(blockSize, TIMESTAMP_FIELD_NAME, null, null, null, false));

        assertSame(counter, timestamp);
    }

    public void testIpDimensionFieldUsesBlockSize1024() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final FieldContext context = new FieldContext(
            randomFrom(128, 512),
            randomNonTimestampFieldName(),
            null,
            null,
            MappedFieldType.IP,
            true
        );

        final PipelineConfig config = resolver.resolve(context);

        assertEquals(1024, config.blockSize());
    }

    public void testIpNonDimensionFieldUsesFormatDefaultBlockSize() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int blockSize = randomFrom(128, 512);
        final FieldContext context = new FieldContext(blockSize, randomNonTimestampFieldName(), null, null, MappedFieldType.IP, false);

        final PipelineConfig config = resolver.resolve(context);

        assertEquals(blockSize, config.blockSize());
    }

    public void testIpDimensionBlockSize1024IsCached() {
        final StaticPipelineConfigResolver resolver = StaticPipelineConfigResolver.INSTANCE;
        final int formatBlockSize = randomFrom(128, 512);

        final PipelineConfig first = resolver.resolve(new FieldContext(formatBlockSize, "client.ip", null, null, MappedFieldType.IP, true));
        final PipelineConfig second = resolver.resolve(
            new FieldContext(formatBlockSize, "server.ip", null, null, MappedFieldType.IP, true)
        );

        assertSame(first, second);
        assertEquals(1024, first.blockSize());
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
