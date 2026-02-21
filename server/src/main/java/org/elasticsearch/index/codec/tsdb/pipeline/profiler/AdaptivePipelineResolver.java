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
import org.elasticsearch.index.codec.tsdb.pipeline.BlockSizeResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineResolver;

public final class AdaptivePipelineResolver implements PipelineResolver {

    private static final Logger logger = LogManager.getLogger(AdaptivePipelineResolver.class);

    private final BlockSizeResolver blockSizeResolver;
    private final BlockProfiler profiler;
    private final PipelineSelector selector;

    public AdaptivePipelineResolver(
        final BlockSizeResolver blockSizeResolver,
        final BlockProfiler profiler,
        final PipelineSelector selector
    ) {
        this.blockSizeResolver = blockSizeResolver;
        this.profiler = profiler;
        this.selector = selector;
    }

    @Override
    public PipelineConfig resolve(final FieldContext context, long[] sample, int sampleSize) {
        if (sampleSize == 0) {
            logger.debug("pipeline-select ({}) [{}] -> default (empty sample)", phase(), context.fieldName());
            return PipelineConfig.defaultConfig();
        }

        final int blockSize = blockSizeResolver.resolve(context);
        final BlockProfile profile = profiler.profile(sample, sampleSize);
        final PipelineConfig.DataType dataType = context.dataType();
        final PipelineConfig pipeline = selector.select(profile, blockSize, dataType, context.hint());

        if (logger.isDebugEnabled()) {
            logger.debug(
                "pipeline-select ({}) [{}] {} n={} | runs={} range={} rawGcd={} shiftedGcd={} mono={} | raw={}b xor={}b dd={}b -> {}",
                phase(),
                context.fieldName(),
                dataType,
                profile.valueCount(),
                profile.runCount(),
                profile.range(),
                profile.rawGcd(),
                profile.shiftedGcd(),
                profile.isMonotonicallyIncreasing() ? "inc" : profile.isMonotonicallyDecreasing() ? "dec" : "no",
                profile.rawMaxBits(),
                profile.xorMaxBits(),
                profile.deltaDeltaMaxBits(),
                pipeline.describeStages()
            );
        }

        return pipeline;
    }

    private static String phase() {
        final String threadName = Thread.currentThread().getName();
        if (threadName.contains("[merge]")) {
            return "merge";
        }
        if (threadName.contains("[flush]")) {
            return "flush";
        }
        if (threadName.contains("[refresh]")) {
            return "refresh";
        }
        return "other";
    }

}
