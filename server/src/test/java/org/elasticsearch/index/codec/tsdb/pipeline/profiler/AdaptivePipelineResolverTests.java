/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.profiler;

import org.elasticsearch.index.codec.tsdb.pipeline.NumericDataGenerators;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineResolver;
import org.elasticsearch.index.mapper.TimeSeriesParams.MetricType;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class AdaptivePipelineResolverTests extends ESTestCase {

    private static final int BLOCK_SIZE = 512;

    private final AdaptivePipelineResolver resolver = new AdaptivePipelineResolver(BlockProfiler.INSTANCE, PipelineSelector.INSTANCE);

    public void testSelectsPipelineForMonotonicLongs() {
        final long[] values = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            values[i] = i * 10L;
        }

        final PipelineResolver.FieldContext ctx = longContext("test");
        final PipelineConfig config = resolver.resolve(ctx, values, BLOCK_SIZE);
        assertFalse(config.isDefault());
    }

    public void testSelectsPipelineForConstantData() {
        final long[] values = new long[BLOCK_SIZE];
        Arrays.fill(values, 42L);

        final PipelineResolver.FieldContext ctx = longContext("test");
        final PipelineConfig config = resolver.resolve(ctx, values, BLOCK_SIZE);
        assertFalse(config.isDefault());
    }

    public void testSelectsPipelineForDoubleGauge() {
        final double[] doubles = NumericDataGenerators.gaugeDoubles(BLOCK_SIZE);
        final long[] values = NumericDataGenerators.doublesToSortableLongs(doubles);

        final PipelineResolver.FieldContext ctx = doubleGaugeContext("cpu.usage");
        final PipelineConfig config = resolver.resolve(ctx, values, BLOCK_SIZE);
        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
    }

    public void testFallsBackToDefaultWhenSampleEmpty() {
        final PipelineResolver.FieldContext ctx = longContext("test");
        final PipelineConfig config = resolver.resolve(ctx, new long[0], 0);
        assertTrue(config.isDefault());
    }

    public void testDeterministic() {
        final long[] values = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            values[i] = i * 10L;
        }

        final PipelineResolver.FieldContext ctx = longContext("test");
        final PipelineConfig first = resolver.resolve(ctx, values, BLOCK_SIZE);
        final PipelineConfig second = resolver.resolve(ctx, values, BLOCK_SIZE);
        assertEquals(first, second);
    }

    public void testSelectsPipelineForFloatGauge() {
        final long[] values = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            values[i] = Float.floatToRawIntBits(65.0f + (i % 100) * 0.1f);
        }

        final PipelineResolver.FieldContext ctx = floatGaugeContext("temperature");
        final PipelineConfig config = resolver.resolve(ctx, values, BLOCK_SIZE);
        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
    }

    public void testInfersLongForIntegerFieldType() {
        final long[] values = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            values[i] = i * 10L;
        }

        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "count",
            null,
            PipelineConfig.DataType.LONG,
            null,
            MetricType.GAUGE,
            false,
            BLOCK_SIZE
        );
        final PipelineConfig config = resolver.resolve(ctx, values, BLOCK_SIZE);
        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.LONG, config.dataType());
    }

    public void testInfersLongForNullFieldType() {
        final long[] values = new long[BLOCK_SIZE];
        for (int i = 0; i < BLOCK_SIZE; i++) {
            values[i] = i * 10L;
        }

        final PipelineResolver.FieldContext ctx = longContext("test");
        final PipelineConfig config = resolver.resolve(ctx, values, BLOCK_SIZE);
        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.LONG, config.dataType());
    }

    private static PipelineResolver.FieldContext longContext(final String fieldName) {
        return new PipelineResolver.FieldContext(fieldName, null, PipelineConfig.DataType.LONG, null, null, false, BLOCK_SIZE);
    }

    private static PipelineResolver.FieldContext doubleGaugeContext(final String fieldName) {
        return new PipelineResolver.FieldContext(
            fieldName,
            null,
            PipelineConfig.DataType.DOUBLE,
            null,
            MetricType.GAUGE,
            false,
            BLOCK_SIZE
        );
    }

    private static PipelineResolver.FieldContext floatGaugeContext(final String fieldName) {
        return new PipelineResolver.FieldContext(fieldName, null, PipelineConfig.DataType.FLOAT, null, MetricType.GAUGE, false, BLOCK_SIZE);
    }
}
