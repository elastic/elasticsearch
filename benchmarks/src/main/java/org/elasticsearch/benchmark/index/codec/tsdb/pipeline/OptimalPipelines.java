/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.pipeline;

import org.elasticsearch.index.codec.tsdb.es94.ES94TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodec;

import java.util.Map;
import java.util.function.Supplier;

public final class OptimalPipelines {

    private static final int BLOCK_SIZE = ES94TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE;

    private static final Map<String, Supplier<NumericCodec>> PIPELINES = Map.ofEntries(
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

    private OptimalPipelines() {}

    public static NumericCodec byName(final String name) {
        final Supplier<NumericCodec> supplier = PIPELINES.get(name);
        if (supplier == null) {
            throw new IllegalArgumentException("Unknown pipeline: " + name);
        }
        return supplier.get();
    }

    // delta->offset->gcd->bit-pack (ES87-compatible)
    public static NumericCodec full() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).delta().offset().gcd().bitPack().build();
    }

    // delta->offset->bit-pack
    public static NumericCodec timestamp() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).delta().offset().bitPack().build();
    }

    // delta->offset->bit-pack
    public static NumericCodec increasing() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).delta().offset().bitPack().build();
    }

    // delta->offset->bit-pack
    public static NumericCodec decreasing() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).delta().offset().bitPack().build();
    }

    // offset->gcd->bit-pack
    public static NumericCodec gcdFriendly() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).offset().gcd().bitPack().build();
    }

    // offset->bit-pack
    public static NumericCodec gaugeLike() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).offset().bitPack().build();
    }

    // offset->bit-pack
    public static NumericCodec constant() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).offset().bitPack().build();
    }

    // offset->bit-pack
    public static NumericCodec lowCardinality() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).offset().bitPack().build();
    }

    // offset->bit-pack
    public static NumericCodec nearConstant() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).offset().bitPack().build();
    }

    // delta->offset->bit-pack
    public static NumericCodec counterWithResets() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).delta().offset().bitPack().build();
    }

    // offset->bit-pack
    public static NumericCodec nonSorted() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).offset().bitPack().build();
    }

    // delta->offset->gcd->bit-pack
    public static NumericCodec allStages() {
        return NumericCodec.withBlockSize(BLOCK_SIZE).delta().offset().gcd().bitPack().build();
    }
}
