/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.engine.Engine;

import static org.elasticsearch.core.Strings.format;

public class IndexingFailuresDebugListener implements IndexingOperationListener {

    private static final Logger LOGGER = LogManager.getLogger(IndexingFailuresDebugListener.class);

    private final IndexShard indexShard;

    public IndexingFailuresDebugListener(IndexShard indexShard) {
        this.indexShard = indexShard;
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        if (LOGGER.isDebugEnabled()) {
            if (result.getResultType() == Engine.Result.Type.FAILURE) {
                postIndex(shardId, index, result.getFailure());
            }
        }
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Exception ex) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                () -> format(
                    "index-fail [%s] seq# [%s] allocation-id [%s] primaryTerm [%s] operationPrimaryTerm [%s] origin [%s]",
                    index.id(),
                    index.seqNo(),
                    indexShard.routingEntry().allocationId(),
                    index.primaryTerm(),
                    indexShard.getOperationPrimaryTerm(),
                    index.origin()
                ),
                ex
            );
        }
    }
}
