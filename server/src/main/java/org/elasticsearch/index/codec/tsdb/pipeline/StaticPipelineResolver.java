/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.apache.lucene.store.IOContext;
import org.elasticsearch.index.IndexMode;

public final class StaticPipelineResolver implements PipelineResolver {

    private static final int TSDB_BLOCK_SIZE = 512;
    private static final int LOGSDB_BLOCK_SIZE = 128;

    public static final StaticPipelineResolver INSTANCE = new StaticPipelineResolver();

    @Override
    public PipelineConfig resolve(final FieldContext context, long[] sample, int sampleSize, IOContext ioContext) {
        final int blockSize = blockSizeFor(context.indexMode());
        return defaultPipeline(context.dataType(), blockSize);
    }

    private static int blockSizeFor(final IndexMode indexMode) {
        if (indexMode == IndexMode.TIME_SERIES) {
            return TSDB_BLOCK_SIZE;
        }
        if (indexMode == IndexMode.LOGSDB) {
            return LOGSDB_BLOCK_SIZE;
        }
        return LOGSDB_BLOCK_SIZE;
    }

    private static PipelineConfig defaultPipeline(final PipelineDescriptor.DataType dataType, int blockSize) {
        return switch (dataType) {
            case LONG -> PipelineConfig.forLongs(blockSize).delta().delta().offset().gcd().patchedPFor().bitPack();
            case DOUBLE -> PipelineConfig.forDoubles(blockSize).delta().delta().offset().gcd().patchedPFor().bitPack();
            case FLOAT -> PipelineConfig.forFloats(blockSize).delta().delta().offset().gcd().patchedPFor().bitPack();
        };
    }
}
