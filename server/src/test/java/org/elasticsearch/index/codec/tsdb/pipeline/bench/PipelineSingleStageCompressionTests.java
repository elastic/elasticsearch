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
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.bench.PipelineTestUtils.DataType;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericDecoder;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

// NOTE: Compares single-stage pipelines to dual baselines (ES87 legacy + ES87-pipeline).
// Now multi-seed with grouped output.
public class PipelineSingleStageCompressionTests extends NumericPipelineTestCase {

    public void testCompareDoubleStages() throws IOException {
        runMultiSeedComparison(
            DataType.DOUBLE,
            PipelineSingleStageCompressionTests::doublePipelineDefs,
            PipelineSingleStageCompressionTests::doubleDataSources
        );
    }

    private void runMultiSeedComparison(
        final DataType testType,
        final java.util.function.Supplier<List<StageDef>> pipelineDefsSupplier,
        final BiFunction<Integer, Long, List<DataSource>> dataSourceFactory
    ) throws IOException {
        final long[] seeds = MultiSeedBenchSupport.getSeeds();
        final List<StageDef> stageDefs = pipelineDefsSupplier.get();
        if (testType != DataType.ANY) {
            for (final StageDef sd : stageDefs) {
                if (sd.dataType() == DataType.ANY) {
                    throw new IllegalArgumentException("StageDef '" + sd.name() + "' uses DataType.ANY in a " + testType + " test.");
                }
            }
        }

        // NOTE: pipeline index 0 = ES87 legacy, index 1 = ES87-pipeline, rest = single-stage pipelines.
        final int totalPipelines = 2 + stageDefs.size();

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

            // NOTE: rebuild codecs per seed.
            final List<Pipeline> pipelines = new ArrayList<>();
            pipelines.add(es87Pipeline());
            pipelines.add(es87PipelineBaseline(BLOCK_SIZE));
            for (final StageDef sd : stageDefs) {
                pipelines.add(encoderPipeline(sd.name, sd.maxError, sd.factory));
            }
            if (s == 0) {
                for (final Pipeline p : pipelines) {
                    pipelineNames.add(p.name);
                }
                pipelineMetas.add(new MultiSeedBenchSupport.PipelineMeta(DataType.ANY, false, 1, true, false));
                pipelineMetas.add(new MultiSeedBenchSupport.PipelineMeta(DataType.ANY, false, 4, false, true));
                for (int i = 0; i < stageDefs.size(); i++) {
                    final Pipeline p = pipelines.get(2 + i);
                    final StageDef sd = stageDefs.get(i);
                    pipelineMetas.add(new MultiSeedBenchSupport.PipelineMeta(sd.dataType, sd.maxError > 0, p.stageCount, false, false));
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
        MultiSeedBenchSupport.writeGroupedOutput("single-stage", seeds, pipelineNames, pipelineMetas, dataNames, dataTypes, allResults);
    }

    // -- Single-stage pipeline definitions --

    private static List<StageDef> doublePipelineDefs() {
        final List<StageDef> list = new ArrayList<>();
        list.add(StageDef.forDoubles("delta+bitPack", b -> b.delta().bitPack()));
        list.add(StageDef.forDoubles("delta+zstd", b -> b.delta().zstd()));
        list.add(StageDef.forDoubles("offset+bitPack", b -> b.offset().bitPack()));
        list.add(StageDef.forDoubles("offset+zstd", b -> b.offset().zstd()));
        list.add(StageDef.forDoubles("gcd+bitPack", b -> b.gcd().bitPack()));
        list.add(StageDef.forDoubles("gcd+zstd", b -> b.gcd().zstd()));
        list.add(StageDef.forDoubles("xor+bitPack", b -> b.xor().bitPack()));
        list.add(StageDef.forDoubles("xor+zstd", b -> b.xor().zstd()));
        list.add(StageDef.forDoubles("patchedPFor+bitPack", b -> b.patchedPFor().bitPack()));
        list.add(StageDef.forDoubles("patchedPFor+zstd", b -> b.patchedPFor().zstd()));
        list.add(StageDef.forDoubles("alp-double", b -> b.alpDouble()));
        list.add(StageDef.forDoubles("alpDoubleStage+offset+gcd+bitPack", b -> b.alpDoubleStage().offset().gcd().bitPack()));
        list.add(StageDef.forDoubles("alpDoubleStage+gcd+bitPack", b -> b.alpDoubleStage().gcd().bitPack()));
        list.add(StageDef.forDoubles("alpRdDoubleStage+offset+gcd+bitPack", b -> b.alpRdDoubleStage().offset().gcd().bitPack()));
        list.add(StageDef.forDoubles("alpRdDoubleStage+gcd+bitPack", b -> b.alpRdDoubleStage().gcd().bitPack()));
        list.add(StageDef.forQuantizedDoubles("quantize(0.01)+bitPack (lossy)", 0.01, b -> b.bitPack()));
        list.add(StageDef.forQuantizedDoubles("quantize(0.01)+zstd (lossy)", 0.01, b -> b.zstd()));
        list.add(StageDef.forDoubles("gorilla (terminal)", b -> b.gorilla()));
        list.add(StageDef.forDoubles("rlePayload (terminal)", b -> b.rlePayload()));
        list.add(StageDef.forDoubles("rle+bitPack", b -> b.rle().bitPack()));
        list.add(StageDef.forDoubles("rle+zstd", b -> b.rle().zstd()));
        return list;
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
        return codecPipelineFromDef(baseline, blockSize);
    }

    private static Pipeline codecPipelineFromDef(final PipelineTestUtils.PipelineDef def, final int blockSize) {
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

    private static Pipeline encoderPipeline(
        final String name,
        final double maxError,
        final java.util.function.IntFunction<NumericEncoder> factory
    ) {
        final NumericEncoder numericEncoder = factory.apply(BLOCK_SIZE);
        final var enc = numericEncoder.newBlockEncoder();
        final NumericDecoder numericDecoder = NumericDecoder.fromDescriptor(numericEncoder.descriptor());
        final var dec = numericDecoder.newBlockDecoder();
        return new Pipeline(name, numericEncoder.descriptor().pipelineLength(), numericEncoder, values -> {
            final byte[] buffer = new byte[BLOCK_SIZE * Long.BYTES * 4];
            final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
            enc.encode(values, values.length, out);
            return new EncodeResult(Arrays.copyOf(buffer, out.getPosition()), out.getPosition());
        }, (encodedBytes, encodedSize) -> {
            final long[] decoded = new long[BLOCK_SIZE];
            dec.decode(decoded, new ByteArrayDataInput(encodedBytes, 0, encodedSize));
            return decoded;
        }, maxError);
    }

    // NOTE: StageDef carries DataType for grouping. Unlike PipelineDef, these are inline single-stage definitions.
    private record StageDef(String name, DataType dataType, double maxError, java.util.function.IntFunction<NumericEncoder> factory) {

        static StageDef forDoubles(
            final String name,
            final java.util.function.Function<PipelineConfig.DoubleBuilder, PipelineConfig> recipe
        ) {
            return new StageDef(name, DataType.DOUBLE, 0.0, bs -> NumericEncoder.fromConfig(recipe.apply(PipelineConfig.forDoubles(bs))));
        }

        static StageDef forQuantizedDoubles(
            final String name,
            final double maxError,
            final java.util.function.Function<PipelineConfig.DoubleBuilder, PipelineConfig> recipe
        ) {
            return new StageDef(
                name,
                DataType.DOUBLE,
                maxError,
                bs -> NumericEncoder.fromConfig(recipe.apply(PipelineConfig.forDoubles(bs).quantizeDouble(maxError)))
            );
        }
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
