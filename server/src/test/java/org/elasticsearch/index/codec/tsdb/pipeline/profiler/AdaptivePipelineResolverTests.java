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
import org.elasticsearch.index.codec.tsdb.pipeline.StaticBlockSizeResolver;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.TimeSeriesParams.MetricType;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;

public class AdaptivePipelineResolverTests extends ESTestCase {

    private final AdaptivePipelineResolver resolver = new AdaptivePipelineResolver(
        StaticBlockSizeResolver.INSTANCE,
        BlockProfiler.INSTANCE,
        PipelineSelector.INSTANCE
    );

    public void testSelectsPipelineForMonotonicLongs() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = i * 10L;
        }

        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext("test", null, null, null, PipelineConfig.DataType.LONG);
        final PipelineConfig config = resolver.resolve(ctx, values, 512);
        assertFalse(config.isDefault());
    }

    public void testSelectsPipelineForConstantData() {
        final long[] values = new long[512];
        Arrays.fill(values, 42L);

        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext("test", null, null, null, PipelineConfig.DataType.LONG);
        final PipelineConfig config = resolver.resolve(ctx, values, 512);
        assertFalse(config.isDefault());
    }

    public void testSelectsPipelineForDoubleGauge() {
        final double[] doubles = NumericDataGenerators.gaugeDoubles(512);
        final long[] values = NumericDataGenerators.doublesToSortableLongs(doubles);

        final MappedFieldType fieldType = createNumberFieldType("cpu.usage", NumberType.DOUBLE, MetricType.GAUGE);
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "cpu.usage",
            null,
            fieldType,
            null,
            PipelineConfig.DataType.DOUBLE
        );
        final PipelineConfig config = resolver.resolve(ctx, values, 512);
        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
    }

    public void testFallsBackToDefaultWhenSampleEmpty() {
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext("test", null, null, null, PipelineConfig.DataType.LONG);
        final PipelineConfig config = resolver.resolve(ctx, new long[0], 0);
        assertTrue(config.isDefault());
    }

    public void testDeterministic() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = i * 10L;
        }

        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext("test", null, null, null, PipelineConfig.DataType.LONG);
        final PipelineConfig first = resolver.resolve(ctx, values, 512);
        final PipelineConfig second = resolver.resolve(ctx, values, 512);
        assertEquals(first, second);
    }

    public void testSelectsPipelineForFloatGauge() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = Float.floatToRawIntBits(65.0f + (i % 100) * 0.1f);
        }

        final MappedFieldType fieldType = createNumberFieldType("temperature", NumberType.FLOAT, MetricType.GAUGE);
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "temperature",
            null,
            fieldType,
            null,
            PipelineConfig.DataType.FLOAT
        );
        final PipelineConfig config = resolver.resolve(ctx, values, 512);
        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
    }

    public void testInfersLongForIntegerFieldType() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = i * 10L;
        }

        final MappedFieldType fieldType = createNumberFieldType("count", NumberType.LONG, MetricType.GAUGE);
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "count",
            null,
            fieldType,
            null,
            PipelineConfig.DataType.LONG
        );
        final PipelineConfig config = resolver.resolve(ctx, values, 512);
        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.LONG, config.dataType());
    }

    public void testInfersLongForNullFieldType() {
        final long[] values = new long[512];
        for (int i = 0; i < 512; i++) {
            values[i] = i * 10L;
        }

        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext("test", null, null, null, PipelineConfig.DataType.LONG);
        final PipelineConfig config = resolver.resolve(ctx, values, 512);
        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.LONG, config.dataType());
    }

    private static MappedFieldType createNumberFieldType(final String name, final NumberType numberType, final MetricType metricType) {
        return new NumberFieldType(
            name,
            numberType,
            IndexType.points(true, true),
            false,
            true,
            null,
            Collections.emptyMap(),
            null,
            false,
            metricType,
            null,
            false,
            null
        );
    }
}
