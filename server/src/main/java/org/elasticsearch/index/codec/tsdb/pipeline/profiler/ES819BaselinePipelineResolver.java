/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline.profiler;

import org.apache.lucene.store.IOContext;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineConfig;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineResolver;

// NOTE: returns the ES819-equivalent pipeline (delta → offset → gcd → bitPack) for all
// data types. Used to measure the pure ES94 framework overhead (per-block bitmap, pipeline
// descriptor) against the ES819 hardcoded encoding path.
public final class ES819BaselinePipelineResolver implements PipelineResolver {

    public static final ES819BaselinePipelineResolver INSTANCE = new ES819BaselinePipelineResolver();

    @Override
    public PipelineConfig resolve(final FieldContext context, long[] sample, int sampleSize, IOContext ioContext) {
        if (sampleSize == 0) {
            return PipelineConfig.defaultConfig();
        }
        final int blockSize = context.blockSize();
        return switch (context.dataType()) {
            case LONG -> PipelineConfig.forLongs(blockSize).delta().offset().gcd().bitPack();
            case DOUBLE -> PipelineConfig.forDoubles(blockSize).delta().offset().gcd().bitPack();
            case FLOAT -> PipelineConfig.forFloats(blockSize).delta().offset().gcd().bitPack();
        };
    }
}
