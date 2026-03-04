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

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class StaticPipelineResolverTests extends ESTestCase {

    private static final long[] EMPTY_SAMPLE = new long[0];
    private static final int TSDB_BLOCK_SIZE = 512;
    private static final int LOGSDB_BLOCK_SIZE = 128;

    private static final List<Class<? extends StageSpec>> DEFAULT_PIPELINE_STAGES = List.of(
        StageSpec.Delta.class,
        StageSpec.Delta.class,
        StageSpec.Offset.class,
        StageSpec.Gcd.class,
        StageSpec.PatchedPFor.class,
        StageSpec.BitPack.class
    );

    private final PipelineResolver resolver = StaticPipelineResolver.INSTANCE;

    public void testTsdbUsesBlockSize512() {
        final PipelineResolver.FieldContext ctx = tsdbContext("cpu.usage", PipelineDescriptor.DataType.DOUBLE, MetricType.GAUGE);
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertThat(config.blockSize(), equalTo(TSDB_BLOCK_SIZE));
    }

    public void testLogsdbUsesBlockSize128() {
        final PipelineResolver.FieldContext ctx = logsdbContext("latency", PipelineDescriptor.DataType.DOUBLE);
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertThat(config.blockSize(), equalTo(LOGSDB_BLOCK_SIZE));
    }

    public void testStandardUsesBlockSize128() {
        final PipelineResolver.FieldContext ctx = standardContext("value", PipelineDescriptor.DataType.LONG);
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertThat(config.blockSize(), equalTo(LOGSDB_BLOCK_SIZE));
    }

    public void testPreservesLongDataType() {
        final PipelineResolver.FieldContext ctx = tsdbContext("requests.total", PipelineDescriptor.DataType.LONG, MetricType.COUNTER);
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertThat(config.dataType(), equalTo(PipelineDescriptor.DataType.LONG));
        assertDefaultPipeline(config);
    }

    public void testPreservesDoubleDataType() {
        final PipelineResolver.FieldContext ctx = tsdbContext("cpu.usage", PipelineDescriptor.DataType.DOUBLE, MetricType.GAUGE);
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertThat(config.dataType(), equalTo(PipelineDescriptor.DataType.DOUBLE));
        assertDefaultPipeline(config);
    }

    public void testPreservesFloatDataType() {
        final PipelineResolver.FieldContext ctx = tsdbContext("temperature", PipelineDescriptor.DataType.FLOAT, MetricType.GAUGE);
        final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);

        assertThat(config.dataType(), equalTo(PipelineDescriptor.DataType.FLOAT));
        assertDefaultPipeline(config);
    }

    public void testIgnoresMetricType() {
        final PipelineConfig gauge = resolver.resolve(
            tsdbContext("field", PipelineDescriptor.DataType.DOUBLE, MetricType.GAUGE),
            EMPTY_SAMPLE,
            0,
            IOContext.DEFAULT
        );
        final PipelineConfig counter = resolver.resolve(
            tsdbContext("field", PipelineDescriptor.DataType.DOUBLE, MetricType.COUNTER),
            EMPTY_SAMPLE,
            0,
            IOContext.DEFAULT
        );

        assertEquals(gauge, counter);
    }

    public void testIgnoresOptimizeForHint() {
        final PipelineConfig noHint = resolver.resolve(
            tsdbContext("field", PipelineDescriptor.DataType.DOUBLE, MetricType.GAUGE),
            EMPTY_SAMPLE,
            0,
            IOContext.DEFAULT
        );
        final PipelineConfig storage = resolver.resolve(
            new PipelineResolver.FieldContext(
                "field",
                IndexMode.TIME_SERIES,
                PipelineDescriptor.DataType.DOUBLE,
                PipelineResolver.OptimizeFor.STORAGE,
                MetricType.GAUGE,
                false,
                TSDB_BLOCK_SIZE
            ),
            EMPTY_SAMPLE,
            0,
            IOContext.DEFAULT
        );

        assertEquals(noHint, storage);
    }

    public void testAlwaysReturnsDefaultPipelineForAllIndexModes() {
        final PipelineResolver.FieldContext[] contexts = {
            tsdbContext("field", PipelineDescriptor.DataType.LONG, MetricType.COUNTER),
            tsdbContext("field", PipelineDescriptor.DataType.DOUBLE, MetricType.GAUGE),
            tsdbContext("field", PipelineDescriptor.DataType.FLOAT, MetricType.GAUGE),
            logsdbContext("field", PipelineDescriptor.DataType.LONG),
            logsdbContext("field", PipelineDescriptor.DataType.DOUBLE),
            logsdbContext("field", PipelineDescriptor.DataType.FLOAT),
            standardContext("field", PipelineDescriptor.DataType.LONG) };

        for (final PipelineResolver.FieldContext ctx : contexts) {
            final PipelineConfig config = resolver.resolve(ctx, EMPTY_SAMPLE, 0, IOContext.DEFAULT);
            assertDefaultPipeline(config);
        }
    }

    private static void assertDefaultPipeline(final PipelineConfig config) {
        final List<StageSpec> specs = config.specs();
        assertThat(specs.size(), equalTo(DEFAULT_PIPELINE_STAGES.size()));
        for (int i = 0; i < specs.size(); i++) {
            assertThat(
                "Stage " + i + " should be " + DEFAULT_PIPELINE_STAGES.get(i).getSimpleName(),
                specs.get(i).getClass(),
                equalTo(DEFAULT_PIPELINE_STAGES.get(i))
            );
        }
    }

    private static PipelineResolver.FieldContext tsdbContext(
        final String fieldName,
        final PipelineDescriptor.DataType dataType,
        final MetricType metricType
    ) {
        return new PipelineResolver.FieldContext(fieldName, IndexMode.TIME_SERIES, dataType, null, metricType, false, TSDB_BLOCK_SIZE);
    }

    private static PipelineResolver.FieldContext logsdbContext(final String fieldName, final PipelineDescriptor.DataType dataType) {
        return new PipelineResolver.FieldContext(fieldName, IndexMode.LOGSDB, dataType, null, null, false, LOGSDB_BLOCK_SIZE);
    }

    private static PipelineResolver.FieldContext standardContext(final String fieldName, final PipelineDescriptor.DataType dataType) {
        return new PipelineResolver.FieldContext(fieldName, IndexMode.STANDARD, dataType, null, null, false, 128);
    }
}
