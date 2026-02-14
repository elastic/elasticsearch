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

public class DefaultPipelineResolverTests extends ESTestCase {

    private final PipelineResolver resolver = new DefaultPipelineResolver();

    public void testTsdbDoubleGaugeSelectsAlp() {
        final var fieldType = createNumberFieldType("cpu.usage", NumberType.DOUBLE, MetricType.GAUGE);
        final var ctx = new PipelineResolver.FieldContext(IndexMode.TIME_SERIES, fieldType, null);
        final PipelineConfig config = resolver.resolve("cpu.usage", ctx);

        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.blockSize(), greaterThan(0));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpDoubleStage.class)));
    }

    public void testTsdbFloatGaugeSelectsAlpFloat() {
        final var fieldType = createNumberFieldType("temperature", NumberType.FLOAT, MetricType.GAUGE);
        final var ctx = new PipelineResolver.FieldContext(IndexMode.TIME_SERIES, fieldType, null);
        final PipelineConfig config = resolver.resolve("temperature", ctx);

        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpFloatStage.class)));
    }

    public void testTsdbLongCounterReturnsDefault() {
        final var fieldType = createNumberFieldType("requests.total", NumberType.LONG, MetricType.COUNTER);
        final var ctx = new PipelineResolver.FieldContext(IndexMode.TIME_SERIES, fieldType, null);
        final PipelineConfig config = resolver.resolve("requests.total", ctx);

        assertTrue(config.isDefault());
    }

    public void testLogsdbDoubleSelectsAlp() {
        final var fieldType = new NumberFieldType("latency", NumberType.DOUBLE);
        final var ctx = new PipelineResolver.FieldContext(IndexMode.LOGSDB, fieldType, null);
        final PipelineConfig config = resolver.resolve("latency", ctx);

        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpDoubleStage.class)));
    }

    public void testLogsdbFloatSelectsAlpFloat() {
        final var fieldType = new NumberFieldType("latency", NumberType.FLOAT);
        final var ctx = new PipelineResolver.FieldContext(IndexMode.LOGSDB, fieldType, null);
        final PipelineConfig config = resolver.resolve("latency", ctx);

        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpFloatStage.class)));
    }

    public void testTsdbHalfFloatGaugeReturnsDefault() {
        final var fieldType = createNumberFieldType("field", NumberType.HALF_FLOAT, MetricType.GAUGE);
        final var ctx = new PipelineResolver.FieldContext(IndexMode.TIME_SERIES, fieldType, null);
        final PipelineConfig config = resolver.resolve("field", ctx);

        assertTrue(config.isDefault());
    }

    public void testLogsdbHalfFloatReturnsDefault() {
        final var fieldType = new NumberFieldType("field", NumberType.HALF_FLOAT);
        final var ctx = new PipelineResolver.FieldContext(IndexMode.LOGSDB, fieldType, null);
        final PipelineConfig config = resolver.resolve("field", ctx);

        assertTrue(config.isDefault());
    }

    public void testLogsdbWithNullMetricTypeReturnsDefault() {
        final var fieldType = new NumberFieldType("status_code", NumberType.LONG);
        final var ctx = new PipelineResolver.FieldContext(IndexMode.LOGSDB, fieldType, null);
        final PipelineConfig config = resolver.resolve("status_code", ctx);

        assertTrue(config.isDefault());
    }

    public void testStandardIndexReturnsDefault() {
        final var fieldType = createNumberFieldType("counter", NumberType.LONG, MetricType.COUNTER);
        final var ctx = new PipelineResolver.FieldContext(IndexMode.STANDARD, fieldType, null);
        final PipelineConfig config = resolver.resolve("counter", ctx);

        assertTrue(config.isDefault());
    }

    public void testLogsdbUsesSmallBlockSize() {
        final var fieldType = new NumberFieldType("latency", NumberType.DOUBLE);
        final var ctx = new PipelineResolver.FieldContext(IndexMode.LOGSDB, fieldType, null);
        final PipelineConfig config = resolver.resolve("latency", ctx);

        assertEquals(128, config.blockSize());
    }

    public void testTsdbUsesLargerBlockSize() {
        final var fieldType = createNumberFieldType("cpu.usage", NumberType.DOUBLE, MetricType.GAUGE);
        final var ctx = new PipelineResolver.FieldContext(IndexMode.TIME_SERIES, fieldType, null);
        final PipelineConfig config = resolver.resolve("cpu.usage", ctx);

        assertEquals(512, config.blockSize());
    }

    public void testTsdbTimestampSelectsDeltaDelta() {
        final var fieldType = new NumberFieldType("@timestamp", NumberType.LONG);
        final var ctx = new PipelineResolver.FieldContext(IndexMode.TIME_SERIES, fieldType, null);
        final PipelineConfig config = resolver.resolve("@timestamp", ctx);

        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.LONG, config.dataType());
        assertEquals(512, config.blockSize());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.DeltaDelta.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Offset.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gcd.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.BitPack.class)));
    }

    public void testLogsdbTimestampSelectsDeltaDelta() {
        final var fieldType = new NumberFieldType("@timestamp", NumberType.LONG);
        final var ctx = new PipelineResolver.FieldContext(IndexMode.LOGSDB, fieldType, null);
        final PipelineConfig config = resolver.resolve("@timestamp", ctx);

        assertFalse(config.isDefault());
        assertEquals(PipelineConfig.DataType.LONG, config.dataType());
        assertEquals(128, config.blockSize());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.DeltaDelta.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Offset.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.Gcd.class)));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.BitPack.class)));
    }

    public void testStandardTimestampReturnsDefault() {
        final var fieldType = new NumberFieldType("@timestamp", NumberType.LONG);
        final var ctx = new PipelineResolver.FieldContext(IndexMode.STANDARD, fieldType, null);
        final PipelineConfig config = resolver.resolve("@timestamp", ctx);

        assertTrue(config.isDefault());
    }

    public void testResolvedPipelinesAreValidForRegistry() {
        final var doubleGaugeType = createNumberFieldType("field", NumberType.DOUBLE, MetricType.GAUGE);
        final var floatGaugeType = createNumberFieldType("field", NumberType.FLOAT, MetricType.GAUGE);
        final var doubleType = new NumberFieldType("field", NumberType.DOUBLE);
        final var floatType = new NumberFieldType("field", NumberType.FLOAT);
        final var longType = new NumberFieldType("field", NumberType.LONG);

        final PipelineResolver.FieldContext[] contexts = {
            new PipelineResolver.FieldContext(IndexMode.TIME_SERIES, doubleGaugeType, null),
            new PipelineResolver.FieldContext(IndexMode.TIME_SERIES, floatGaugeType, null),
            new PipelineResolver.FieldContext(IndexMode.LOGSDB, doubleType, null),
            new PipelineResolver.FieldContext(IndexMode.LOGSDB, floatType, null),
            new PipelineResolver.FieldContext(IndexMode.LOGSDB, longType, null) };

        for (final var ctx : contexts) {
            final PipelineConfig config = resolver.resolve("field", ctx);
            if (config.isDefault() == false) {
                assertFalse("Config should be valid for registry", config.isDefault());
            }
        }
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
