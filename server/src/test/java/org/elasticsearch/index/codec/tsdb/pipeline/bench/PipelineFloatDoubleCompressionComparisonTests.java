/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.bench;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.NumericPipelineTestCase;
import org.elasticsearch.index.codec.tsdb.pipeline.bench.PipelineTestUtils.DataType;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

// NOTE: Compares double pipelines to ES87 at block size 128.
// Now multi-seed with grouped output and dual baselines.
public class PipelineFloatDoubleCompressionComparisonTests extends NumericPipelineTestCase {

    public void testCompareDoublePipelines() throws IOException {
        runMultiSeedComparison(
            PipelineTestUtils.doublePipelines(),
            DataType.DOUBLE,
            PipelineFloatDoubleCompressionComparisonTests::doubleDataSources
        );
    }

    private void runMultiSeedComparison(
        final List<PipelineTestUtils.PipelineDef> defs,
        final DataType filterType,
        final BiFunction<Integer, Long, List<DataSource>> dataSourceFactory
    ) throws IOException {
        final long[] seeds = MultiSeedBenchSupport.getSeeds();
        PipelineTestUtils.assertNoAnyType(defs);
        final List<PipelineTestUtils.PipelineDef> filtered = new ArrayList<>();
        for (final PipelineTestUtils.PipelineDef def : defs) {
            if (PipelineTestUtils.compatible(def.dataType(), filterType)) {
                filtered.add(def);
            }
        }

        // NOTE: pipeline index 0 = ES87 legacy, index 1 = ES87-pipeline.
        final int totalPipelines = 2 + filtered.size();

        final List<String> dataNames = new ArrayList<>();
        final List<DataType> dataTypes = new ArrayList<>();
        final List<String> pipelineNames = new ArrayList<>();
        final List<MultiSeedBenchSupport.PipelineMeta> pipelineMetas = new ArrayList<>();
        int[][][] allResults = null;

        for (int s = 0; s < seeds.length; s++) {
            final long seed = seeds[s];
            final List<DataSource> dataSources = dataSourceFactory.apply(BLOCK_SIZE, seed);
            if (s == 0) {
                allResults = new int[dataSources.size()][totalPipelines][seeds.length];
                for (final DataSource ds : dataSources) {
                    dataNames.add(ds.name);
                    dataTypes.add(ds.dataType);
                }
            }

            final List<Pipeline> pipelines = new ArrayList<>();
            pipelines.add(es87Pipeline());
            pipelines.add(es87PipelineBaseline(BLOCK_SIZE));
            for (final PipelineTestUtils.PipelineDef def : filtered) {
                pipelines.add(encoderPipeline(def, BLOCK_SIZE));
            }
            if (s == 0) {
                for (final Pipeline p : pipelines) {
                    pipelineNames.add(p.name);
                }
                pipelineMetas.add(new MultiSeedBenchSupport.PipelineMeta(DataType.ANY, false, 1, true, false));
                pipelineMetas.add(new MultiSeedBenchSupport.PipelineMeta(DataType.ANY, false, 4, false, true));
                for (int i = 0; i < filtered.size(); i++) {
                    final Pipeline p = pipelines.get(2 + i);
                    pipelineMetas.add(
                        new MultiSeedBenchSupport.PipelineMeta(
                            filtered.get(i).dataType(),
                            filtered.get(i).maxError() > 0,
                            p.stageCount,
                            false,
                            false
                        )
                    );
                }
            }

            for (int d = 0; d < dataSources.size(); d++) {
                final DataSource ds = dataSources.get(d);
                for (int p = 0; p < pipelines.size(); p++) {
                    final Pipeline pipeline = pipelines.get(p);
                    final EncodeResult encoded = pipeline.encoder.encode(ds.values.clone());
                    final long[] decoded = pipeline.decoder.decode(encoded.encodedBytes, encoded.encodedSize);
                    if (pipeline.maxError > 0) {
                        PipelineTestUtils.assertArrayEqualsWithTolerance(
                            pipeline.name + " round-trip failed for " + ds.name + " seed=" + seed,
                            ds.values,
                            decoded,
                            pipeline.maxError,
                            ds.dataType
                        );
                    } else {
                        assertArrayEquals(pipeline.name + " round-trip failed for " + ds.name + " seed=" + seed, ds.values, decoded);
                    }
                    allResults[d][p][s] = encoded.encodedSize;
                }
            }

            for (final Pipeline pipeline : pipelines) {
                if (pipeline.numericEncoder != null) {
                    pipeline.numericEncoder.close();
                }
            }
        }

        final String output = MultiSeedBenchSupport.formatGroupedResultTable(
            seeds,
            pipelineNames,
            pipelineMetas,
            dataNames,
            dataTypes,
            allResults
        );
        logger.info(output);
        MultiSeedBenchSupport.writeGroupedOutput("comparison", seeds, pipelineNames, pipelineMetas, dataNames, dataTypes, allResults);
    }

    // -- Seeded data source factories --

    private static List<DataSource> doubleDataSources(final int blockSize, final long seed) {
        final List<DataSource> sources = new ArrayList<>();
        for (final var ds : NumericDataGenerators.seededDoubleDataSources()) {
            final double[] doubles = ds.generator().apply(blockSize, seed);
            final long[] sortable = new long[doubles.length];
            for (int i = 0; i < doubles.length; i++) {
                sortable[i] = NumericUtils.doubleToSortableLong(doubles[i]);
            }
            sources.add(new DataSource(ds.name(), DataType.DOUBLE, sortable));
        }
        return sources;
    }

    // -- Baseline and codec pipeline factories --

    private Pipeline es87Pipeline() {
        final TSDBDocValuesEncoder encoder = new TSDBDocValuesEncoder(BLOCK_SIZE);
        return new Pipeline("ES87", 1, null, values -> {
            final byte[] buffer = new byte[BLOCK_SIZE * Long.BYTES + 256];
            final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
            encoder.encode(values, out);
            return new EncodeResult(Arrays.copyOf(buffer, out.getPosition()), out.getPosition());
        }, (encodedBytes, encodedSize) -> {
            final long[] decoded = new long[BLOCK_SIZE];
            encoder.decode(new ByteArrayDataInput(encodedBytes, 0, encodedSize), decoded);
            return decoded;
        }, 0.0);
    }

    private static Pipeline es87PipelineBaseline(final int blockSize) {
        final PipelineTestUtils.PipelineDef baseline = PipelineTestUtils.es87PipelineBaselineForDoubles();
        return encoderPipeline(baseline, blockSize);
    }

    private static Pipeline encoderPipeline(final PipelineTestUtils.PipelineDef def, final int blockSize) {
        final NumericEncoder numericEncoder = def.factory().apply(blockSize);
        final var enc = numericEncoder.newBlockEncoder();
        final NumericDecoder numericDecoder = NumericDecoder.fromDescriptor(numericEncoder.descriptor());
        final var dec = numericDecoder.newBlockDecoder();
        return new Pipeline(def.name(), numericEncoder.descriptor().pipelineLength(), numericEncoder, values -> {
            final byte[] buffer = new byte[blockSize * Long.BYTES * 4];
            final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
            enc.encode(values, values.length, out);
            return new EncodeResult(Arrays.copyOf(buffer, out.getPosition()), out.getPosition());
        }, (encodedBytes, encodedSize) -> {
            final long[] decoded = new long[blockSize];
            dec.decode(decoded, new ByteArrayDataInput(encodedBytes, 0, encodedSize));
            return decoded;
        }, def.maxError());
    }

    private record DataSource(String name, DataType dataType, long[] values) {}

    private record Pipeline(
        String name,
        int stageCount,
        NumericEncoder numericEncoder,
        Encoder encoder,
        Decoder decoder,
        double maxError
    ) {}

    private record EncodeResult(byte[] encodedBytes, int encodedSize) {}

    @FunctionalInterface
    private interface Encoder {
        EncodeResult encode(long[] values) throws IOException;
    }

    @FunctionalInterface
    private interface Decoder {
        long[] decode(byte[] encodedBytes, int encodedSize) throws IOException;
    }
}
