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
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.VersionConflictEngineException;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.elasticsearch.core.Strings.format;

public class IndexingFailuresDiagnosticListener implements IndexingOperationListener {

    private static final Logger LOGGER = LogManager.getLogger(IndexingFailuresDiagnosticListener.class);

    private static final int MAX_WARNINGS_PER_WINDOW = 5;
    private static final long RATE_LIMIT_WINDOW_NANOS = TimeUnit.HOURS.toNanos(1);

    private final IndexShard indexShard;
    private final LongSupplier nanoTimeSupplier;
    // High 32 bits: index of the current rate limit window. Low 32 bits: permits used in that window.
    private final AtomicLong windowAndPermitsUsed = new AtomicLong();

    public IndexingFailuresDiagnosticListener(IndexShard indexShard) {
        this(indexShard, System::nanoTime);
    }

    // Visible for testing
    IndexingFailuresDiagnosticListener(IndexShard indexShard, LongSupplier nanoTimeSupplier) {
        this.indexShard = indexShard;
        this.nanoTimeSupplier = nanoTimeSupplier;
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
                    "index-fail [%s] seq# [%s] allocation-id [%s] primaryTerm [%s] operationPrimaryTerm [%s] origin [%s] "
                        + "index [%s] index-mode [%s]",
                    index.id(),
                    index.seqNo(),
                    indexShard.routingEntry().allocationId(),
                    index.primaryTerm(),
                    indexShard.getOperationPrimaryTerm(),
                    index.origin(),
                    indexShard.indexSettings().getIndex().getName(),
                    indexShard.indexSettings().getMode()
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
        if (isUserError(ex)) {
            LOGGER.trace(getMessageSupplier("index-fail", batch, i), ex);
        } else {
            Supplier<Object> messageSupplier = getMessageSupplier("real-index-fail", batch, i);
            if (withinRateLimit()) {
                LOGGER.warn(messageSupplier, ex);
            } else {
                LOGGER.debug(messageSupplier, ex);
            }
        }
    }

    boolean withinRateLimit() {
        int window = (int) Math.floorDiv(nanoTimeSupplier.getAsLong(), RATE_LIMIT_WINDOW_NANOS);
        while (true) {
            long state = windowAndPermitsUsed.get();
            int permitsUsed = (int) (state >> 32) == window ? (int) state : 0;
            if (permitsUsed >= MAX_WARNINGS_PER_WINDOW) {
                return false;
            }
            long newState = ((long) window << 32) | (permitsUsed + 1);
            if (windowAndPermitsUsed.compareAndSet(state, newState)) {
                return true;
            }
        }
    }

    private Supplier<Object> getMessageSupplier(String messagePrefix, IndexOperationBatch batch, int i) {
        return () -> format(
            "%s [%s] seq# [%s] allocation-id [%s] primaryTerm [%s] operationPrimaryTerm [%s] origin [%s] index [%s] index-mode [%s]",
            messagePrefix,
            batch.id(i),
            batch.seqNo(i),
            indexShard.routingEntry().allocationId(),
            batch.primaryTerm(),
            indexShard.getOperationPrimaryTerm(),
            batch.origin(),
            indexShard.indexSettings().getIndex().getName(),
            indexShard.indexSettings().getMode()
        );
    }

    /**
     * Returns true if the exception is a user error, false otherwise.
     * User errors are logged at trace level, while other errors such as codec issues or lucene errors are logged at debug level.
     * @return true if the exception is a user error, false otherwise
     */
    private boolean isUserError(Exception ex) {
        return switch (ex) {
            case DocumentMissingException dme -> true;
            case VersionConflictEngineException vcee -> true;
            case IllegalArgumentException iae -> InternalEngine.isTooManyDocsFailure(iae);
            default -> false;
        };
    }

    private void logEntireBatchFailure(IndexOperationBatch batch, Exception ex) {
        LOGGER.debug(
            () -> format(
                "index-fail batch docCount [%s] startingSeqNo [%s] allocation-id [%s] primaryTerm [%s] "
                    + "operationPrimaryTerm [%s] origin [%s] index [%s] index-mode [%s]",
                batch.docCount(),
                batch.seqNo(0),
                indexShard.routingEntry().allocationId(),
                batch.primaryTerm(),
                indexShard.getOperationPrimaryTerm(),
                batch.origin(),
                indexShard.indexSettings().getIndex().getName(),
                indexShard.indexSettings().getMode()
            ),
            ex
        );
    }
}
