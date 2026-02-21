/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.profiler;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.codec.tsdb.pipeline.NumericDataGenerators;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.StaticPipelineResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;
import org.elasticsearch.index.mapper.TimeSeriesParams.MetricType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class PipelineDiscoveryTests extends ESTestCase {

    private static final int BLOCK_SIZE = 512;
    private static final long SEED = 0x5DEECE66DL;

    private final AdaptivePipelineResolver adaptive = new AdaptivePipelineResolver(BlockProfiler.INSTANCE, PipelineSelector.INSTANCE);
    private final StaticPipelineResolver staticResolver = StaticPipelineResolver.INSTANCE;

    public void testCompareAdaptiveVsStaticOnAllLongGenerators() throws IOException {
        for (final NumericDataGenerators.SeededLongDataSource ds : NumericDataGenerators.seededLongDataSources()) {
            final long[] values = ds.generator().apply(BLOCK_SIZE, SEED);
            final PipelineResolver.FieldContext ctx = new PipelineResolver.FieldContext(
                ds.name(),
                null,
                PipelineConfig.DataType.LONG,
                null,
                null,
                false,
                BLOCK_SIZE
            );
            final PipelineConfig adaptiveConfig = adaptive.resolve(ctx, values, BLOCK_SIZE, IOContext.DEFAULT);
            final PipelineConfig staticConfig = staticResolver.resolve(ctx, values, BLOCK_SIZE, IOContext.DEFAULT);

            final int adaptiveBytes = measureSize(adaptiveConfig, values);
            final int staticBytes = staticConfig.isDefault()
                ? measureSize(PipelineConfig.forLongs(BLOCK_SIZE).delta().offset().gcd().bitPack(), values)
                : measureSize(staticConfig, values);

            final int delta = staticBytes > 0 ? ((staticBytes - adaptiveBytes) * 100 / staticBytes) : 0;
            logger.info(
                "[LONG] {} | adaptive={} bytes ({}) | static={} bytes ({}) | delta={}%",
                ds.name(),
                adaptiveBytes,
                adaptiveConfig,
                staticBytes,
                staticConfig,
                delta
            );
        }
    }

    public void testCompareAdaptiveVsStaticOnAllDoubleGenerators() throws IOException {
        for (final NumericDataGenerators.SeededDoubleDataSource ds : NumericDataGenerators.seededDoubleDataSources()) {
            final long[] values = NumericDataGenerators.doublesToSortableLongs(ds.generator().apply(BLOCK_SIZE, SEED));
            final PipelineResolver.FieldContext ctx = buildDoubleGaugeContext(ds.name());
            final PipelineConfig adaptiveConfig = adaptive.resolve(ctx, values, BLOCK_SIZE, IOContext.DEFAULT);

            final int adaptiveBytes = measureSize(adaptiveConfig, values);
            logger.info("[DOUBLE] {} | adaptive={} bytes ({})", ds.name(), adaptiveBytes, adaptiveConfig);
        }
    }

    public void testValidateFixedMappingsWithTrialEncode() throws IOException {
        final List<PipelineConfig> candidatePipelines = List.of(
            PipelineConfig.forLongs(BLOCK_SIZE).delta().offset().gcd().bitPack(),
            PipelineConfig.forLongs(BLOCK_SIZE).deltaDelta().offset().gcd().patchedPFor().bitPack(),
            PipelineConfig.forLongs(BLOCK_SIZE).offset().gcd().bitPack(),
            PipelineConfig.forLongs(BLOCK_SIZE).offset().rle().bitPack(),
            PipelineConfig.forLongs(BLOCK_SIZE).offset().bitPack(),
            PipelineConfig.forLongs(BLOCK_SIZE).delta().offset().bitPack()
        );

        for (final NumericDataGenerators.SeededLongDataSource ds : NumericDataGenerators.seededLongDataSources()) {
            final long[] values = ds.generator().apply(BLOCK_SIZE, SEED);
            final BlockProfile profile = BlockProfiler.INSTANCE.profile(values, BLOCK_SIZE);
            final PipelineConfig selectedConfig = PipelineSelector.INSTANCE.select(
                profile,
                BLOCK_SIZE,
                PipelineConfig.DataType.LONG,
                null,
                null
            );

            final int selectedBytes = measureSize(selectedConfig, values);
            int bestBytes = Integer.MAX_VALUE;
            PipelineConfig bestConfig = null;
            for (final PipelineConfig candidate : candidatePipelines) {
                final int bytes = measureSize(candidate, values);
                if (bytes < bestBytes) {
                    bestBytes = bytes;
                    bestConfig = candidate;
                }
            }

            final double overhead = bestBytes > 0 ? (double) (selectedBytes - bestBytes) / bestBytes * 100 : 0;
            logger.info(
                "[VALIDATE] {} | selected={} ({} bytes) | optimal={} ({} bytes) | overhead={} %",
                ds.name(),
                selectedConfig,
                selectedBytes,
                bestConfig,
                bestBytes,
                String.format("%.1f", overhead)
            );
        }
    }

    private static int measureSize(final PipelineConfig config, final long[] values) throws IOException {
        final int blockSize = config.blockSize();
        final long[] block = Arrays.copyOf(values, blockSize);
        final byte[] buffer = new byte[blockSize * Long.BYTES * 8];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        final NumericEncoder encoder = NumericEncoder.fromConfig(config);
        encoder.newBlockEncoder().encode(block, Math.min(values.length, blockSize), out);
        return out.getPosition();
    }

    private static PipelineResolver.FieldContext buildDoubleGaugeContext(final String fieldName) {
        return new PipelineResolver.FieldContext(
            fieldName,
            IndexMode.TIME_SERIES,
            PipelineConfig.DataType.DOUBLE,
            null,
            MetricType.GAUGE,
            false,
            BLOCK_SIZE
        );
    }
}
