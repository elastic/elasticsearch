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
import org.apache.lucene.store.MergeInfo;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineResolver;

public final class AdaptivePipelineResolver implements PipelineResolver {

    private static final Logger logger = LogManager.getLogger(AdaptivePipelineResolver.class);

    private final BlockProfiler profiler;
    private final PipelineSelector selector;

    public AdaptivePipelineResolver(final BlockProfiler profiler, final PipelineSelector selector) {
        this.profiler = profiler;
        this.selector = selector;
    }

    @Override
    public PipelineConfig resolve(final FieldContext context, long[] sample, int sampleSize, IOContext ioContext) {
        if (sampleSize == 0) {
            logger.debug("pipeline-select ({}) [{}] -> default (empty sample)", phase(ioContext), context.fieldName());
            return PipelineConfig.defaultConfig();
        }

        final int blockSize = context.blockSize();
        final BlockProfile profile = profiler.profile(sample, sampleSize);
        final PipelineConfig.DataType dataType = context.dataType();
        final PipelineConfig pipeline = selector.select(profile, blockSize, dataType, context.hint(), context.metricType());

        if (logger.isDebugEnabled()) {
            final String hint = context.hint() != null ? context.hint().name().toLowerCase() : "none";
            final String mono = profile.isMonotonicallyIncreasing() ? "inc" : profile.isMonotonicallyDecreasing() ? "dec" : "no";
            logger.debug(
                "pipeline-select [{}] phase={} type={} hint={} n={}"
                    + " | profile: runs={} range={} gcd={}/{} mono={} bits=[raw={} xor={} dd={}]"
                    + " | -> {}",
                context.fieldName(),
                phase(ioContext),
                dataType,
                hint,
                profile.valueCount(),
                profile.runCount(),
                profile.range(),
                profile.rawGcd(),
                profile.shiftedGcd(),
                mono,
                profile.rawMaxBits(),
                profile.xorMaxBits(),
                profile.deltaDeltaMaxBits(),
                pipeline.describeStages()
            );
        }

        return pipeline;
    }

    private static String phase(IOContext ioContext) {
        final MergeInfo mergeInfo = ioContext.mergeInfo();
        if (mergeInfo != null) {
            return mergeInfo.mergeMaxNumSegments() == -1 ? "merge" : "force-merge";
        }
        if (ioContext.flushInfo() != null) {
            return "flush";
        }
        return "other";
    }

}
