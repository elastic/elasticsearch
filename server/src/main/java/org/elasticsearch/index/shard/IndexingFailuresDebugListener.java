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
import org.elasticsearch.index.engine.IndexOperationBatch;

import java.util.List;

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

    @Override
    public IndexOperationBatch preIndexBatch(ShardId shardId, IndexOperationBatch batch) {
        // no pre-work; overridden to avoid the delegating to default
        return batch;
    }

    /**
     * Batch equivalent of {@link #postIndex(ShardId, Engine.Index, Engine.IndexResult)}: failed
     * documents are logged from the batch's raw fields.
     */
    @Override
    public void postIndexBatch(ShardId shardId, IndexOperationBatch batch, List<Engine.IndexResult> results) {
        if (LOGGER.isDebugEnabled()) {
            for (int i = 0; i < results.size(); i++) {
                Engine.IndexResult result = results.get(i);
                if (result.getResultType() == Engine.Result.Type.FAILURE) {
                    logBatchOpFailure(batch, i, result.getFailure());
                }
            }
        }
    }

    @Override
    public void postIndexBatch(ShardId shardId, IndexOperationBatch batch, Exception ex) {
        if (LOGGER.isDebugEnabled()) {
            logEntireBatchFailure(batch, ex);
        }
    }

    private void logBatchOpFailure(IndexOperationBatch batch, int i, Exception ex) {
        LOGGER.debug(
            () -> format(
                "index-fail [%s] seq# [%s] allocation-id [%s] primaryTerm [%s] operationPrimaryTerm [%s] origin [%s]",
                batch.id(i),
                batch.seqNo(i),
                indexShard.routingEntry().allocationId(),
                batch.primaryTerm(),
                indexShard.getOperationPrimaryTerm(),
                batch.origin()
            ),
            ex
        );
    }

    private void logEntireBatchFailure(IndexOperationBatch batch, Exception ex) {
        LOGGER.debug(
            () -> format(
                "index-fail batch docCount [%s] startingSeqNo [%s] allocation-id [%s] primaryTerm [%s] "
                    + "operationPrimaryTerm [%s] origin [%s]",
                batch.docCount(),
                batch.seqNo(0),
                indexShard.routingEntry().allocationId(),
                batch.primaryTerm(),
                indexShard.getOperationPrimaryTerm(),
                batch.origin()
            ),
            ex
        );
    }
}
