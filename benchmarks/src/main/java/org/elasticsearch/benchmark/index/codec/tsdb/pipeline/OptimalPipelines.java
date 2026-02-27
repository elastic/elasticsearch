/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.pipeline;

import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;

import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.function.Supplier;

public final class OptimalPipelines {

    private static final int BLOCK_SIZE = ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE;

    private static final Map<String, Supplier<PipelineConfig>> LEGACY = Map.ofEntries(
        Map.entry("full", OptimalPipelines::full),
        Map.entry("allStages", OptimalPipelines::allStages),
        Map.entry("timestamp", OptimalPipelines::timestamp),
        Map.entry("increasing", OptimalPipelines::increasing),
        Map.entry("decreasing", OptimalPipelines::decreasing),
        Map.entry("gcdFriendly", OptimalPipelines::gcdFriendly),
        Map.entry("gaugeLike", OptimalPipelines::gaugeLike),
        Map.entry("constant", OptimalPipelines::constant),
        Map.entry("lowCardinality", OptimalPipelines::lowCardinality),
        Map.entry("nearConstant", OptimalPipelines::nearConstant),
        Map.entry("counterWithResets", OptimalPipelines::counterWithResets),
        Map.entry("nonSorted", OptimalPipelines::nonSorted)
    );

    private static final Map<String, IntFunction<PipelineConfig>> DOUBLE_SHORTLIST = Map.ofEntries(
        // ALP payload stages (monolithic encode/decode)
        Map.entry("alp_double", bs -> PipelineConfig.forDoubles(bs).alpDouble()),
        Map.entry("alp_rd_double", bs -> PipelineConfig.forDoubles(bs).alpRdDouble()),
        // ALP transform + bitpack pipelines
        Map.entry("alp_double_stage-offset-gcd-bitpack", bs -> PipelineConfig.forDoubles(bs).alpDoubleStage().offset().gcd().bitPack()),
        Map.entry(
            "alp_rd_double_stage-offset-gcd-bitpack",
            bs -> PipelineConfig.forDoubles(bs).alpRdDoubleStage().offset().gcd().bitPack()
        ),
        // Quantize pipelines
        Map.entry(
            "quantize-1e6-delta-offset-gcd-bitpack",
            bs -> PipelineConfig.forDoubles(bs).quantizeDouble(1e-6).delta().offset().gcd().bitPack()
        ),
        Map.entry(
            "alp_double_stage-1e6-offset-gcd-bitpack",
            bs -> PipelineConfig.forDoubles(bs).alpDoubleStage(1e-6).offset().gcd().bitPack()
        ),
        // Baselines
        Map.entry("gorilla", bs -> PipelineConfig.forDoubles(bs).gorilla()),
        Map.entry("xor-bitpack", bs -> PipelineConfig.forDoubles(bs).xor().bitPack()),
        Map.entry("offset-gcd-bitpack", bs -> PipelineConfig.forDoubles(bs).offset().gcd().bitPack()),
        Map.entry("zstd", bs -> PipelineConfig.forDoubles(bs).zstd()),
        // Integer-path (raw long bits)
        Map.entry("delta-offset-gcd-bitpack", bs -> PipelineConfig.forDoubles(bs).delta().offset().gcd().bitPack()),
        Map.entry("delta-offset-gcd-zstd", bs -> PipelineConfig.forDoubles(bs).delta().offset().gcd().zstd()),
        // ALP + zstd
        Map.entry("alp_double_stage-offset-gcd-zstd", bs -> PipelineConfig.forDoubles(bs).alpDoubleStage().offset().gcd().zstd()),
        Map.entry("alp_double_stage-1e6-zstd", bs -> PipelineConfig.forDoubles(bs).alpDoubleStage(1e-6).zstd()),
        // FPC pipelines
        Map.entry("fpc-offset-gcd-bitpack", bs -> PipelineConfig.forDoubles(bs).fpcStage().offset().gcd().bitPack()),
        Map.entry("fpc-bitpack", bs -> PipelineConfig.forDoubles(bs).fpcStage().bitPack()),
        Map.entry("fpc-zstd", bs -> PipelineConfig.forDoubles(bs).fpcStage().zstd()),
        Map.entry("fpc-offset-gcd-zstd", bs -> PipelineConfig.forDoubles(bs).fpcStage().offset().gcd().zstd()),
        Map.entry(
            "quantize-1e6-fpc-offset-gcd-bitpack",
            bs -> PipelineConfig.forDoubles(bs).quantizeDouble(1e-6).fpcStage().offset().gcd().bitPack()
        ),
        // LZ4 pipelines
        Map.entry("lz4", bs -> PipelineConfig.forDoubles(bs).lz4()),
        Map.entry("lz4-hc", bs -> PipelineConfig.forDoubles(bs).lz4HighCompression()),
        Map.entry("xor-lz4", bs -> PipelineConfig.forDoubles(bs).xor().lz4()),
        Map.entry("delta-offset-gcd-lz4", bs -> PipelineConfig.forDoubles(bs).delta().offset().gcd().lz4()),
        Map.entry("alp_double_stage-offset-gcd-lz4", bs -> PipelineConfig.forDoubles(bs).alpDoubleStage().offset().gcd().lz4()),
        Map.entry("fpc-lz4", bs -> PipelineConfig.forDoubles(bs).fpcStage().lz4()),
        Map.entry("fpc-offset-gcd-lz4", bs -> PipelineConfig.forDoubles(bs).fpcStage().offset().gcd().lz4()),
        // Chimp128 payload pipelines
        Map.entry("chimp128-double", bs -> PipelineConfig.forDoubles(bs).chimp128())
    );

    private static final Map<String, IntFunction<PipelineConfig>> ALL;
    static {
        var combined = new java.util.HashMap<String, IntFunction<PipelineConfig>>();
        LEGACY.forEach((name, supplier) -> combined.put(name, bs -> supplier.get()));
        DOUBLE_SHORTLIST.forEach(combined::putIfAbsent);
        ALL = Map.copyOf(combined);
    }

    private OptimalPipelines() {}

    public static PipelineConfig byName(final String name, int blockSize) {
        final IntFunction<PipelineConfig> factory = ALL.get(name);
        if (factory == null) {
            throw new IllegalArgumentException("Unknown pipeline: " + name + ". Available: " + ALL.keySet());
        }
        return factory.apply(blockSize);
    }

    public static PipelineConfig byName(final String name) {
        return byName(name, BLOCK_SIZE);
    }

    public static List<String> shortlistDoubleNames() {
        return List.copyOf(DOUBLE_SHORTLIST.keySet());
    }

    public static PipelineConfig full() {
        return PipelineConfig.forLongs(BLOCK_SIZE).delta().offset().gcd().bitPack();
    }

    public static PipelineConfig timestamp() {
        return PipelineConfig.forLongs(BLOCK_SIZE).delta().offset().bitPack();
    }

    public static PipelineConfig increasing() {
        return PipelineConfig.forLongs(BLOCK_SIZE).delta().offset().bitPack();
    }

    public static PipelineConfig decreasing() {
        return PipelineConfig.forLongs(BLOCK_SIZE).delta().offset().bitPack();
    }

    public static PipelineConfig gcdFriendly() {
        return PipelineConfig.forLongs(BLOCK_SIZE).offset().gcd().bitPack();
    }

    public static PipelineConfig gaugeLike() {
        return PipelineConfig.forLongs(BLOCK_SIZE).offset().bitPack();
    }

    public static PipelineConfig constant() {
        return PipelineConfig.forLongs(BLOCK_SIZE).offset().bitPack();
    }

    public static PipelineConfig lowCardinality() {
        return PipelineConfig.forLongs(BLOCK_SIZE).offset().bitPack();
    }

    public static PipelineConfig nearConstant() {
        return PipelineConfig.forLongs(BLOCK_SIZE).offset().bitPack();
    }

    public static PipelineConfig counterWithResets() {
        return PipelineConfig.forLongs(BLOCK_SIZE).delta().offset().bitPack();
    }

    public static PipelineConfig nonSorted() {
        return PipelineConfig.forLongs(BLOCK_SIZE).offset().bitPack();
    }

    public static PipelineConfig allStages() {
        return PipelineConfig.forLongs(BLOCK_SIZE).delta().offset().gcd().bitPack();
    }
}
