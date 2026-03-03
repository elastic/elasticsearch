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

    // NOTE: These 14 pipelines mirror the PipelineSelector decision tree (13 selector paths + 1 ES819-pipeline-reference).
    private static final Map<String, IntFunction<PipelineConfig>> DOUBLE_SHORTLIST = Map.ofEntries(
        Map.entry("delta-offset-gcd-bitpack", bs -> PipelineConfig.forDoubles(bs).delta().offset().gcd().bitPack()),
        Map.entry("integer-pipeline", bs -> PipelineConfig.forDoubles(bs).delta().delta().offset().gcd().patchedPFor().bitPack()),
        Map.entry("alp-double-lossless", bs -> PipelineConfig.forDoubles(bs).alpDoubleStage().delta().offset().gcd().patchedPFor().bitPack()),
        Map.entry("alp-double-1e4", bs -> PipelineConfig.forDoubles(bs).alpDoubleStage(1e-4).delta().offset().gcd().patchedPFor().bitPack()),
        Map.entry("alp-double-1e2", bs -> PipelineConfig.forDoubles(bs).alpDoubleStage(1e-2).delta().offset().gcd().patchedPFor().bitPack()),
        Map.entry("fpc-lossless", bs -> PipelineConfig.forDoubles(bs).fpcStage().delta().offset().gcd().patchedPFor().bitPack()),
        Map.entry("fpc-1e4", bs -> PipelineConfig.forDoubles(bs).fpcStage(1e-4).delta().offset().gcd().patchedPFor().bitPack()),
        Map.entry("fpc-1e2", bs -> PipelineConfig.forDoubles(bs).fpcStage(1e-2).delta().offset().gcd().patchedPFor().bitPack()),
        Map.entry("gorilla-lossless", bs -> PipelineConfig.forDoubles(bs).gorilla()),
        Map.entry("gorilla-1e4", bs -> PipelineConfig.forDoubles(bs).gorilla(1e-4)),
        Map.entry("gorilla-1e2", bs -> PipelineConfig.forDoubles(bs).gorilla(1e-2)),
        Map.entry("chimp-lossless", bs -> PipelineConfig.forDoubles(bs).chimp()),
        Map.entry("chimp-1e4", bs -> PipelineConfig.forDoubles(bs).chimp(1e-4)),
        Map.entry("chimp128-1e2", bs -> PipelineConfig.forDoubles(bs).chimp128(1e-2))
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
