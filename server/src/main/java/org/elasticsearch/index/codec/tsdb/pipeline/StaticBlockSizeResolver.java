/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.pipeline;

import org.elasticsearch.index.IndexMode;

public final class StaticBlockSizeResolver implements BlockSizeResolver {

    public static final StaticBlockSizeResolver INSTANCE = new StaticBlockSizeResolver();

    static final int TSDB_BLOCK_SIZE = 512;
    static final int LOGSDB_BLOCK_SIZE = 128;
    static final int DEFAULT_BLOCK_SIZE = 128;

    @Override
    public int resolve(final PipelineResolver.FieldContext context) {
        if (context.indexMode() == IndexMode.TIME_SERIES) {
            return TSDB_BLOCK_SIZE;
        }
        if (context.indexMode() == IndexMode.LOGSDB) {
            return LOGSDB_BLOCK_SIZE;
        }
        return DEFAULT_BLOCK_SIZE;
    }
}
