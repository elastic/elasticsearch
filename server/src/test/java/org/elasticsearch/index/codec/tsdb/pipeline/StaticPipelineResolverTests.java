/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.TimeSeriesParams.MetricType;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;

public class StaticPipelineResolverTests extends ESTestCase {

    private static final long[] EMPTY_SAMPLE = new long[0];

    private final PipelineResolver resolver = new StaticPipelineResolver(StaticBlockSizeResolver.INSTANCE);

    public void testTsdbDoubleGaugeSelectsAlp() {
        final MappedFieldType fieldType = createNumberFieldType("cpu.usage", NumberType.DOUBLE, MetricType.GAUGE);
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "cpu.usage",
            IndexMode.TIME_SERIES,
            fieldType,
            null,
            PipelineConfig.DataType.DOUBLE
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0);

        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.blockSize(), greaterThan(0));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpDoubleStage.class)));
    }

    public void testTsdbDoubleGaugeBalancedSelectsAlp() {
        final MappedFieldType fieldType = createNumberFieldType("cpu.usage", NumberType.DOUBLE, MetricType.GAUGE);
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "cpu.usage",
            IndexMode.TIME_SERIES,
            fieldType,
            PipelineResolver.OptimizeFor.BALANCED,
            PipelineConfig.DataType.DOUBLE
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0);

        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpDoubleStage.class)));
    }

    public void testTsdbFloatGaugeSelectsAlpFloat() {
        final MappedFieldType fieldType = createNumberFieldType("temperature", NumberType.FLOAT, MetricType.GAUGE);
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "temperature",
            IndexMode.TIME_SERIES,
            fieldType,
            null,
            PipelineConfig.DataType.FLOAT
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0);

        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpFloatStage.class)));
    }

    public void testTsdbFloatGaugeBalancedSelectsAlpFloat() {
        final MappedFieldType fieldType = createNumberFieldType("temperature", NumberType.FLOAT, MetricType.GAUGE);
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "temperature",
            IndexMode.TIME_SERIES,
            fieldType,
            PipelineResolver.OptimizeFor.BALANCED,
            PipelineConfig.DataType.FLOAT
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0);

        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpFloatStage.class)));
    }

    public void testTsdbLongCounterReturnsDefault() {
        final MappedFieldType fieldType = createNumberFieldType("requests.total", NumberType.LONG, MetricType.COUNTER);
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "requests.total",
            IndexMode.TIME_SERIES,
            fieldType,
            null,
            PipelineConfig.DataType.LONG
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0);

        assertTrue(config.isDefault());
    }

    public void testLogsdbDoubleSelectsAlp() {
        final NumberFieldType fieldType = new NumberFieldType("latency", NumberType.DOUBLE);
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "latency",
            IndexMode.LOGSDB,
            fieldType,
            null,
            PipelineConfig.DataType.DOUBLE
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0);

        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpDoubleStage.class)));
    }

    public void testLogsdbFloatSelectsAlpFloat() {
        final NumberFieldType fieldType = new NumberFieldType("latency", NumberType.FLOAT);
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "latency",
            IndexMode.LOGSDB,
            fieldType,
            null,
            PipelineConfig.DataType.FLOAT
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0);

        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpFloatStage.class)));
    }

    public void testTsdbHalfFloatGaugeReturnsDefault() {
        final MappedFieldType fieldType = createNumberFieldType("field", NumberType.HALF_FLOAT, MetricType.GAUGE);
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "field",
            IndexMode.TIME_SERIES,
            fieldType,
            null,
            PipelineConfig.DataType.FLOAT
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0);

        assertTrue(config.isDefault());
    }

    public void testLogsdbHalfFloatReturnsDefault() {
        final NumberFieldType fieldType = new NumberFieldType("field", NumberType.HALF_FLOAT);
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "field",
            IndexMode.LOGSDB,
            fieldType,
            null,
            PipelineConfig.DataType.FLOAT
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0);

        assertTrue(config.isDefault());
    }

    public void testLogsdbWithNullMetricTypeReturnsDefault() {
        final NumberFieldType fieldType = new NumberFieldType("status_code", NumberType.LONG);
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "status_code",
            IndexMode.LOGSDB,
            fieldType,
            null,
            PipelineConfig.DataType.LONG
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0);

        assertTrue(config.isDefault());
    }

    public void testStandardIndexReturnsDefault() {
        final MappedFieldType fieldType = createNumberFieldType("counter", NumberType.LONG, MetricType.COUNTER);
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "counter",
            IndexMode.STANDARD,
            fieldType,
            null,
            PipelineConfig.DataType.LONG
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0);

        assertTrue(config.isDefault());
    }

    public void testLogsdbUsesSmallBlockSize() {
        final NumberFieldType fieldType = new NumberFieldType("latency", NumberType.DOUBLE);
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "latency",
            IndexMode.LOGSDB,
            fieldType,
            null,
            PipelineConfig.DataType.DOUBLE
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0);

        assertEquals(128, config.blockSize());
    }

    public void testTsdbUsesLargerBlockSize() {
        final MappedFieldType fieldType = createNumberFieldType("cpu.usage", NumberType.DOUBLE, MetricType.GAUGE);
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "cpu.usage",
            IndexMode.TIME_SERIES,
            fieldType,
            null,
            PipelineConfig.DataType.DOUBLE
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0);

        assertEquals(512, config.blockSize());
    }

    public void testTsdbDateSelectsDeltaDelta() {
        final MappedFieldType fieldType = createDateFieldType("@timestamp");
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "@timestamp",
            IndexMode.TIME_SERIES,
            fieldType,
            null,
            PipelineConfig.DataType.LONG
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0);

        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.LONG, config.dataType());
        assertEquals(512, config.blockSize());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.DeltaDelta.class)));
    }

    public void testLogsdbDateSelectsDeltaDelta() {
        final MappedFieldType fieldType = createDateFieldType("@timestamp");
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "@timestamp",
            IndexMode.LOGSDB,
            fieldType,
            null,
            PipelineConfig.DataType.LONG
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0);

        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.LONG, config.dataType());
        assertEquals(128, config.blockSize());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.DeltaDelta.class)));
    }

    public void testStandardTimestampReturnsDefault() {
        final MappedFieldType fieldType = createDateFieldType("@timestamp");
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "@timestamp",
            IndexMode.STANDARD,
            fieldType,
            null,
            PipelineConfig.DataType.LONG
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0);

        assertTrue(config.isDefault());
    }

    public void testResolvedPipelinesAreValidForRegistry() {
        final MappedFieldType doubleGaugeType = createNumberFieldType("field", NumberType.DOUBLE, MetricType.GAUGE);
        final MappedFieldType floatGaugeType = createNumberFieldType("field", NumberType.FLOAT, MetricType.GAUGE);
        final NumberFieldType doubleType = new NumberFieldType("field", NumberType.DOUBLE);
        final NumberFieldType floatType = new NumberFieldType("field", NumberType.FLOAT);
        final NumberFieldType longType = new NumberFieldType("field", NumberType.LONG);

        final PipelineResolver.FieldContext[] contexts = {
            new PipelineResolver.FieldContext("field", IndexMode.TIME_SERIES, doubleGaugeType, null, PipelineConfig.DataType.DOUBLE),
            new PipelineResolver.FieldContext("field", IndexMode.TIME_SERIES, floatGaugeType, null, PipelineConfig.DataType.FLOAT),
            new PipelineResolver.FieldContext("field", IndexMode.LOGSDB, doubleType, null, PipelineConfig.DataType.DOUBLE),
            new PipelineResolver.FieldContext("field", IndexMode.LOGSDB, floatType, null, PipelineConfig.DataType.FLOAT),
            new PipelineResolver.FieldContext("field", IndexMode.LOGSDB, longType, null, PipelineConfig.DataType.LONG) };

        for (final PipelineResolver.FieldContext ctx : contexts) {
            final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0);
            if (config.isDefault() == false) {
                assertFalse("Config should be valid for registry", config.isDefault());
            }
        }
    }

    private static MappedFieldType createDateFieldType(final String name) {
        return new DateFieldType(name);
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
