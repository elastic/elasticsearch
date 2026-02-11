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
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;
import org.elasticsearch.index.codec.tsdb.pipeline.NumericPipelineTestCase;
import org.elasticsearch.index.codec.tsdb.pipeline.bench.PipelineTestUtils.DataType;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// NOTE: Compares integer pipelines to ES87 at block size 128.
// Now multi-seed with grouped output and dual baselines.
public class PipelineIntegerCompressionComparisonTests extends NumericPipelineTestCase {

    public void testCompareAllPipelines() throws IOException {
        final long[] seeds = MultiSeedBenchSupport.getSeeds();

        // NOTE: assert all integer pipelines are lossless.
        for (final PipelineTestUtils.PipelineDef def : PipelineTestUtils.integerPipelines()) {
            assert def.maxError() == 0.0 : "integer pipeline must be lossless: " + def.name();
        }

        // NOTE: pipeline index 0 = ES87 legacy, index 1 = ES87-pipeline.
        final List<PipelineTestUtils.PipelineDef> intPipelines = PipelineTestUtils.integerPipelines();
        final int totalPipelines = 2 + intPipelines.size();

        final List<String> dataNames = new ArrayList<>();
        final List<DataType> dataTypes = new ArrayList<>();
        final List<String> pipelineNames = new ArrayList<>();
        final List<MultiSeedBenchSupport.PipelineMeta> pipelineMetas = new ArrayList<>();
        int[][][] allResults = null;

        for (int s = 0; s < seeds.length; s++) {
            final long seed = seeds[s];
            final List<DataSource> dataSources = dataSources(BLOCK_SIZE, seed);
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
            for (final PipelineTestUtils.PipelineDef def : intPipelines) {
                pipelines.add(encoderPipeline(def, BLOCK_SIZE));
            }
            if (s == 0) {
                for (final Pipeline p : pipelines) {
                    pipelineNames.add(p.name);
                }
                pipelineMetas.add(new MultiSeedBenchSupport.PipelineMeta(DataType.ANY, false, 1, true, false));
                pipelineMetas.add(new MultiSeedBenchSupport.PipelineMeta(DataType.ANY, false, 4, false, true));
                for (int i = 0; i < intPipelines.size(); i++) {
                    final Pipeline p = pipelines.get(2 + i);
                    pipelineMetas.add(new MultiSeedBenchSupport.PipelineMeta(DataType.ANY, false, p.stageCount, false, false));
                }
            }

            for (int d = 0; d < dataSources.size(); d++) {
                final DataSource ds = dataSources.get(d);
                for (int p = 0; p < pipelines.size(); p++) {
                    final Pipeline pipeline = pipelines.get(p);
                    final EncodeResult encoded = pipeline.encoder.encode(ds.values.clone());
                    final long[] decoded = pipeline.decoder.decode(encoded.encodedBytes, encoded.encodedSize);
                    assertArrayEquals(pipeline.name + " round-trip failed for " + ds.name + " seed=" + seed, ds.values, decoded);
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
        MultiSeedBenchSupport.writeGroupedOutput("int-comparison", seeds, pipelineNames, pipelineMetas, dataNames, dataTypes, allResults);
    }

    // -- Seeded data sources --

    private static List<DataSource> dataSources(final int blockSize, final long seed) {
        final List<DataSource> sources = new ArrayList<>();
        for (final var ds : NumericDataGenerators.seededLongDataSources()) {
            sources.add(new DataSource(ds.name(), DataType.ANY, ds.generator().apply(blockSize, seed)));
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
        return encoderPipeline(PipelineTestUtils.es87PipelineBaseline(), blockSize);
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

    // NOTE: DataSource now includes DataType for grouping API compatibility.
    private record DataSource(String name, DataType dataType, long[] values) {}

    // NOTE: Pipeline now includes maxError for grouping API compatibility (always 0.0 for integer).
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
