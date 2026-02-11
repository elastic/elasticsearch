/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.bench;

import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericEncoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.test.ESTestCase.assertEquals;
import static org.elasticsearch.test.ESTestCase.assertTrue;

final class PipelineTestUtils {

    private PipelineTestUtils() {}

    enum DataType {
        ANY,
        DOUBLE
    }

    static final double LOSSY_EPSILON = 1e-9;

    record PipelineDef(String name, DataType dataType, double maxError, Function<Integer, NumericEncoder> factory) {

        static PipelineDef forLongs(final String name, final Function<PipelineConfig.LongBuilder, PipelineConfig> recipe) {
            return new PipelineDef(name, DataType.ANY, 0.0, bs -> NumericEncoder.fromConfig(recipe.apply(PipelineConfig.forLongs(bs))));
        }

        static PipelineDef forDoubles(final String name, final Function<PipelineConfig.DoubleBuilder, PipelineConfig> recipe) {
            return new PipelineDef(
                name,
                DataType.DOUBLE,
                0.0,
                bs -> NumericEncoder.fromConfig(recipe.apply(PipelineConfig.forDoubles(bs)))
            );
        }

        static PipelineDef forQuantizedDoubles(
            final String name,
            final double maxError,
            final Function<PipelineConfig.DoubleBuilder, PipelineConfig> recipe
        ) {
            return new PipelineDef(
                name,
                DataType.DOUBLE,
                maxError,
                bs -> NumericEncoder.fromConfig(recipe.apply(PipelineConfig.forDoubles(bs).quantizeDouble(maxError)))
            );
        }
    }

    // NOTE: ES87-pipeline baseline (delta+offset+gcd+bitPack). Same stages as ES87 legacy
    // but with pipeline framing overhead (+1 byte per block for stage-id header).
    // Unlike ES87 legacy, this can be instantiated at any block size.
    static PipelineDef es87PipelineBaseline() {
        return PipelineDef.forLongs("ES87-pipeline", b -> b.delta().offset().gcd().bitPack());
    }

    // NOTE: ES87-pipeline for double typed builder.
    static PipelineDef es87PipelineBaselineForDoubles() {
        return PipelineDef.forDoubles("ES87-pipeline", b -> b.delta().offset().gcd().bitPack());
    }

    // NOTE: returns baselines appropriate for the given block size.
    // At block size 128: [ES87-pipeline] (ES87 legacy is constructed locally by test classes
    // because TSDBDocValuesEncoder cannot implement the NumericEncoder API).
    // At block size != 128: [ES87-pipeline] only. ES87 legacy is fixed to block size 128.
    static List<PipelineDef> baselines(final int blockSize) {
        final List<PipelineDef> list = new ArrayList<>();
        list.add(es87PipelineBaseline());
        return list;
    }

    static List<PipelineDef> integerPipelines() {
        final List<PipelineDef> list = new ArrayList<>();
        list.add(PipelineDef.forLongs("delta+offset+bitPack", b -> b.delta().offset().bitPack()));
        list.add(PipelineDef.forLongs("offset+gcd+bitPack", b -> b.offset().gcd().bitPack()));
        list.add(PipelineDef.forLongs("offset+bitPack", b -> b.offset().bitPack()));
        list.add(PipelineDef.forLongs("delta+offset+zstd", b -> b.delta().offset().zstd()));
        list.add(PipelineDef.forLongs("offset+zstd", b -> b.offset().zstd()));
        list.add(PipelineDef.forLongs("offset+gcd+zstd", b -> b.offset().gcd().zstd()));
        list.add(PipelineDef.forLongs("offset+patchedPFor+bitPack", b -> b.offset().patchedPFor().bitPack()));
        list.add(PipelineDef.forLongs("offset+patchedPFor+zstd", b -> b.offset().patchedPFor().zstd()));
        list.add(PipelineDef.forLongs("xor+bitPack", b -> b.xor().bitPack()));
        list.add(PipelineDef.forLongs("rle+bitPack", b -> b.rle().bitPack()));
        list.add(PipelineDef.forLongs("rle+zstd", b -> b.rle().zstd()));
        list.add(PipelineDef.forLongs("rlePayload", b -> b.rlePayload()));
        list.add(PipelineDef.forLongs("offset+rle+bitPack", b -> b.offset().rle().bitPack()));
        list.add(PipelineDef.forLongs("delta+offset+rle+bitPack", b -> b.delta().offset().rle().bitPack()));
        return list;
    }

    static List<PipelineDef> doublePipelines() {
        final List<PipelineDef> list = new ArrayList<>();
        list.add(PipelineDef.forDoubles("xor+bitPack", b -> b.xor().bitPack()));
        list.add(PipelineDef.forDoubles("gorilla", b -> b.gorilla()));
        list.add(PipelineDef.forDoubles("alp-double", b -> b.alpDouble()));
        list.add(PipelineDef.forDoubles("alp-rd-double", b -> b.alpRdDouble()));
        // NOTE: composable ALP stages -- transform in-place, chain with downstream integer stages.
        list.add(PipelineDef.forDoubles("alpDoubleStage+offset+gcd+bitPack", b -> b.alpDoubleStage().offset().gcd().bitPack()));
        list.add(PipelineDef.forDoubles("alpDoubleStage+gcd+bitPack", b -> b.alpDoubleStage().gcd().bitPack()));
        list.add(PipelineDef.forDoubles("alpRdDoubleStage+offset+gcd+bitPack", b -> b.alpRdDoubleStage().offset().gcd().bitPack()));
        list.add(PipelineDef.forDoubles("alpRdDoubleStage+gcd+bitPack", b -> b.alpRdDoubleStage().gcd().bitPack()));
        // NOTE: shallow (2-3 stage) lossless pipelines for throughput comparison.
        list.add(PipelineDef.forDoubles("delta+offset+bitPack", b -> b.delta().offset().bitPack()));
        list.add(PipelineDef.forDoubles("offset+gcd+bitPack", b -> b.offset().gcd().bitPack()));
        // NOTE: shallow lossy pipelines (quantize + alp terminal, no offset/gcd/ppfor).
        list.add(PipelineDef.forQuantizedDoubles("quantize(0.1)+alp (lossy)", 0.1, b -> b.alpDouble()));
        list.add(PipelineDef.forQuantizedDoubles("quantize(0.01)+alp (lossy)", 0.01, b -> b.alpDouble()));
        // NOTE: zstd variants are lossless alternatives to bitPack. They are only competitive
        // at block sizes >= 1024 where frame header overhead is amortized.
        list.add(PipelineDef.forDoubles("delta+offset+gcd+zstd", b -> b.delta().offset().gcd().zstd()));
        list.add(PipelineDef.forDoubles("delta+offset+zstd", b -> b.delta().offset().zstd()));
        list.add(PipelineDef.forDoubles("offset+gcd+zstd", b -> b.offset().gcd().zstd()));
        list.add(PipelineDef.forDoubles("offset+zstd", b -> b.offset().zstd()));
        list.add(PipelineDef.forQuantizedDoubles("quantize(0.0001)+alp (lossy)", 0.0001, b -> b.alpDouble()));
        list.add(PipelineDef.forQuantizedDoubles("quantize(0.001)+alp (lossy)", 0.001, b -> b.alpDouble()));
        list.add(PipelineDef.forQuantizedDoubles("quantize(0.01)+alp (lossy)", 0.01, b -> b.alpDouble()));
        list.add(PipelineDef.forQuantizedDoubles("quantize(0.1)+alp (lossy)", 0.1, b -> b.alpDouble()));
        // NOTE: RLE pipelines.
        list.add(PipelineDef.forDoubles("rle+bitPack", b -> b.rle().bitPack()));
        list.add(PipelineDef.forDoubles("rle+zstd", b -> b.rle().zstd()));
        list.add(PipelineDef.forDoubles("rlePayload", b -> b.rlePayload()));
        list.add(PipelineDef.forDoubles("offset+rle+bitPack", b -> b.offset().rle().bitPack()));
        list.add(PipelineDef.forDoubles("delta+offset+rle+bitPack", b -> b.delta().offset().rle().bitPack()));
        return list;
    }

    // NOTE: guards against ANY leaking into double bench runs.
    // ANY is valid for integer pipelines (forLongs) but not for typed (double) benches.
    static void assertNoAnyType(final List<PipelineDef> defs) {
        for (final PipelineDef def : defs) {
            if (def.dataType() == DataType.ANY) {
                throw new IllegalArgumentException(
                    "PipelineDef '" + def.name() + "' uses DataType.ANY in a double pipeline list. " + "Use forDoubles() instead."
                );
            }
        }
    }

    static boolean compatible(DataType pipelineType, DataType dataType) {
        return pipelineType == DataType.ANY || pipelineType == dataType;
    }

    static boolean isZstdPipeline(PipelineDef def) {
        return def.name().contains("zstd");
    }

    // NOTE: zstd pipelines should only run at block sizes >= this threshold.
    static final int ZSTD_MIN_BLOCK_SIZE = 1024;

    static void assertArrayEqualsWithTolerance(
        final String message,
        final long[] expected,
        final long[] actual,
        double maxError,
        final DataType dataType
    ) throws IOException {
        assertEquals(message + " (length mismatch)", expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            double e = NumericUtils.sortableLongToDouble(expected[i]);
            double a = NumericUtils.sortableLongToDouble(actual[i]);
            if (Double.isNaN(e)) {
                assertTrue(message + " at index " + i + ": expected NaN", Double.isNaN(a));
            } else {
                double diff = Math.abs(e - a);
                assertTrue(
                    message + " at index " + i + ": expected=" + e + " actual=" + a + " diff=" + diff,
                    diff <= maxError + LOSSY_EPSILON
                );
            }
        }
    }
}
