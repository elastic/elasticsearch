/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.profiler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineResolver;

import java.util.function.Function;

public final class StageIsolationResolver implements PipelineResolver {

    private static final Logger logger = LogManager.getLogger(StageIsolationResolver.class);

    private final String label;
    private final Function<Integer, PipelineConfig> longPipeline;
    private final Function<Integer, PipelineConfig> doublePipeline;
    private final Function<Integer, PipelineConfig> floatPipeline;

    private StageIsolationResolver(
        String label,
        Function<Integer, PipelineConfig> longPipeline,
        Function<Integer, PipelineConfig> doublePipeline,
        Function<Integer, PipelineConfig> floatPipeline
    ) {
        this.label = label;
        this.longPipeline = longPipeline;
        this.doublePipeline = doublePipeline;
        this.floatPipeline = floatPipeline;
    }

    // NOTE: ES819-equivalent pipeline: delta > offset > gcd > bitPack
    public static StageIsolationResolver es819Baseline() {
        return new StageIsolationResolver(
            "es819-baseline",
            bs -> PipelineConfig.forLongs(bs).delta().offset().gcd().bitPack(),
            bs -> PipelineConfig.forDoubles(bs).delta().offset().gcd().bitPack(),
            bs -> PipelineConfig.forFloats(bs).delta().offset().gcd().bitPack()
        );
    }

    // NOTE: adds second delta to isolate its cost: delta > delta > offset > gcd > bitPack
    public static StageIsolationResolver withDoubleDelta() {
        return new StageIsolationResolver(
            "double-delta",
            bs -> PipelineConfig.forLongs(bs).delta().delta().offset().gcd().bitPack(),
            bs -> PipelineConfig.forDoubles(bs).delta().delta().offset().gcd().bitPack(),
            bs -> PipelineConfig.forFloats(bs).delta().delta().offset().gcd().bitPack()
        );
    }

    // NOTE: adds PFor on top of double delta: delta > delta > offset > gcd > pfor > bitPack
    public static StageIsolationResolver withDoubleDeltaAndPFor() {
        return new StageIsolationResolver(
            "double-delta-pfor",
            bs -> PipelineConfig.forLongs(bs).delta().delta().offset().gcd().patchedPFor().bitPack(),
            bs -> PipelineConfig.forDoubles(bs).delta().delta().offset().gcd().patchedPFor().bitPack(),
            bs -> PipelineConfig.forFloats(bs).delta().delta().offset().gcd().patchedPFor().bitPack()
        );
    }

    @Override
    public PipelineConfig resolve(final FieldContext context, long[] sample, int sampleSize, IOContext ioContext) {
        if (sampleSize == 0) {
            return PipelineConfig.defaultConfig();
        }
        final int blockSize = context.blockSize();
        final PipelineConfig config = switch (context.dataType()) {
            case LONG -> longPipeline.apply(blockSize);
            case DOUBLE -> doublePipeline.apply(blockSize);
            case FLOAT -> floatPipeline.apply(blockSize);
        };
        logger.debug("field [{}] isolation=[{}] -> [{}]", context.fieldName(), label, config);
        return config;
    }
}
