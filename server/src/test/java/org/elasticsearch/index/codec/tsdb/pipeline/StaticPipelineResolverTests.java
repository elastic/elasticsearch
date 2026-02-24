/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.store.IOContext;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.TimeSeriesParams.MetricType;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;

public class StaticPipelineResolverTests extends ESTestCase {

    private static final long[] EMPTY_SAMPLE = new long[0];
    private static final int TSDB_BLOCK_SIZE = 512;
    private static final int LOGSDB_BLOCK_SIZE = 128;

    private final PipelineResolver resolver = StaticPipelineResolver.INSTANCE;

    public void testTsdbDoubleGaugeSelectsAlp() {
        final PipelineResolver.FieldContext ctx = tsdbContext("cpu.usage", PipelineConfig.DataType.DOUBLE, MetricType.GAUGE);
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.blockSize(), greaterThan(0));
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpDoubleStage.class)));
    }

    public void testTsdbDoubleGaugeBalancedSelectsAlp() {
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "cpu.usage",
            IndexMode.TIME_SERIES,
            PipelineConfig.DataType.DOUBLE,
            PipelineResolver.OptimizeFor.BALANCED,
            MetricType.GAUGE,
            false,
            TSDB_BLOCK_SIZE
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpDoubleStage.class)));
    }

    public void testTsdbFloatGaugeSelectsAlpFloat() {
        final PipelineResolver.FieldContext ctx = tsdbContext("temperature", PipelineConfig.DataType.FLOAT, MetricType.GAUGE);
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpFloatStage.class)));
    }

    public void testTsdbFloatGaugeBalancedSelectsAlpFloat() {
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "temperature",
            IndexMode.TIME_SERIES,
            PipelineConfig.DataType.FLOAT,
            PipelineResolver.OptimizeFor.BALANCED,
            MetricType.GAUGE,
            false,
            TSDB_BLOCK_SIZE
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpFloatStage.class)));
    }

    public void testTsdbLongCounterReturnsBaseline() {
        final PipelineResolver.FieldContext ctx = tsdbContext("requests.total", PipelineConfig.DataType.LONG, MetricType.COUNTER);
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertEquals(PipelineConfig.forLongs(TSDB_BLOCK_SIZE).delta().offset().gcd().bitPack(), config);
    }

    public void testLogsdbDoubleSelectsAlp() {
        final PipelineResolver.FieldContext ctx = logsdbContext("latency", PipelineConfig.DataType.DOUBLE);
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertEquals(PipelineConfig.DataType.DOUBLE, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpDoubleStage.class)));
    }

    public void testLogsdbFloatSelectsAlpFloat() {
        final PipelineResolver.FieldContext ctx = logsdbContext("latency", PipelineConfig.DataType.FLOAT);
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertEquals(PipelineConfig.DataType.FLOAT, config.dataType());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.AlpFloatStage.class)));
    }

    public void testLogsdbWithNullMetricTypeReturnsBaseline() {
        final PipelineResolver.FieldContext ctx = logsdbContext("status_code", PipelineConfig.DataType.LONG);
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertEquals(PipelineConfig.forLongs(LOGSDB_BLOCK_SIZE).delta().offset().gcd().bitPack(), config);
    }

    public void testStandardIndexReturnsBaseline() {
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "counter",
            IndexMode.STANDARD,
            PipelineConfig.DataType.LONG,
            null,
            MetricType.COUNTER,
            false,
            128
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertEquals(PipelineConfig.forLongs(128).delta().offset().gcd().bitPack(), config);
    }

    public void testLogsdbUsesSmallBlockSize() {
        final PipelineResolver.FieldContext ctx = logsdbContext("latency", PipelineConfig.DataType.DOUBLE);
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertEquals(LOGSDB_BLOCK_SIZE, config.blockSize());
    }

    public void testTsdbUsesLargerBlockSize() {
        final PipelineResolver.FieldContext ctx = tsdbContext("cpu.usage", PipelineConfig.DataType.DOUBLE, MetricType.GAUGE);
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertEquals(TSDB_BLOCK_SIZE, config.blockSize());
    }

    public void testTsdbDateSelectsDeltaDelta() {
        final PipelineResolver.FieldContext ctx = tsdbDateContext("@timestamp");
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertEquals(PipelineConfig.DataType.LONG, config.dataType());
        assertEquals(TSDB_BLOCK_SIZE, config.blockSize());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.DeltaDelta.class)));
    }

    public void testLogsdbDateSelectsDeltaDelta() {
        final PipelineResolver.FieldContext ctx = logsdbDateContext("@timestamp");
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertEquals(PipelineConfig.DataType.LONG, config.dataType());
        assertEquals(LOGSDB_BLOCK_SIZE, config.blockSize());
        assertThat(config.specs(), hasItem(instanceOf(StageSpec.DeltaDelta.class)));
    }

    public void testStandardTimestampReturnsBaseline() {
        final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
            "@timestamp",
            IndexMode.STANDARD,
            PipelineConfig.DataType.LONG,
            null,
            null,
            true,
            128
        );
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertEquals(PipelineConfig.forLongs(128).delta().offset().gcd().bitPack(), config);
    }

    public void testResolvedPipelinesAreValidForRegistry() {
        final PipelineResolver.FieldContext[] contexts = {
            tsdbContext("field", PipelineConfig.DataType.DOUBLE, MetricType.GAUGE),
            tsdbContext("field", PipelineConfig.DataType.FLOAT, MetricType.GAUGE),
            logsdbContext("field", PipelineConfig.DataType.DOUBLE),
            logsdbContext("field", PipelineConfig.DataType.FLOAT),
            logsdbContext("field", PipelineConfig.DataType.LONG) };

        for (final PipelineResolver.FieldContext ctx : contexts) {
            final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);
            assertFalse("Config should have stages", config.specs().isEmpty());
        }
    }

    private static PipelineResolver.FieldContext tsdbContext(
        final String fieldName,
        final PipelineConfig.DataType dataType,
        final MetricType metricType
    ) {
        return new PipelineResolver.FieldContext(fieldName, IndexMode.TIME_SERIES, dataType, null, metricType, false, TSDB_BLOCK_SIZE);
    }

    private static PipelineResolver.FieldContext tsdbDateContext(final String fieldName) {
        return new PipelineResolver.FieldContext(
            fieldName,
            IndexMode.TIME_SERIES,
            PipelineConfig.DataType.LONG,
            null,
            null,
            true,
            TSDB_BLOCK_SIZE
        );
    }

    private static PipelineResolver.FieldContext logsdbContext(final String fieldName, final PipelineConfig.DataType dataType) {
        return new PipelineResolver.FieldContext(fieldName, IndexMode.LOGSDB, dataType, null, null, false, LOGSDB_BLOCK_SIZE);
    }

    private static PipelineResolver.FieldContext logsdbDateContext(final String fieldName) {
        return new PipelineResolver.FieldContext(
            fieldName,
            IndexMode.LOGSDB,
            PipelineConfig.DataType.LONG,
            null,
            null,
            true,
            LOGSDB_BLOCK_SIZE
        );
    }
}
