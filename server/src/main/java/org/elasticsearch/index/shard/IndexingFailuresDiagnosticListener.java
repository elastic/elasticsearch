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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.VersionConflictEngineException;

import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.core.Strings.format;

/**
 * Logs indexing failures to help diagnose them. Real (non user) failures are logged at WARN for the first few failures
 * per hour on the node, and at DEBUG after that, so a persistent problem is visible at the default log level
 * without flooding the log.
 */
public class IndexingFailuresDiagnosticListener implements IndexingOperationListener {

    private static final Logger LOGGER = LogManager.getLogger(IndexingFailuresDiagnosticListener.class);

    private static final WindowedRateLimiter SHARED_WARN_LIMITER = new WindowedRateLimiter(5, TimeValue.ONE_HOUR);

    private final IndexShard indexShard;
    private final WindowedRateLimiter warnLimiter;

    public IndexingFailuresDiagnosticListener(IndexShard indexShard) {
        this(indexShard, SHARED_WARN_LIMITER);
    }

    // Visible for testing
    IndexingFailuresDiagnosticListener(IndexShard indexShard, WindowedRateLimiter warnLimiter) {
        this.indexShard = indexShard;
        this.warnLimiter = warnLimiter;
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        if (result.getResultType() == Engine.Result.Type.FAILURE && (LOGGER.isDebugEnabled() || warnLimiter.mayHavePermit())) {
            postIndex(shardId, index, result.getFailure());
        }
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Exception ex) {
        logFailure(ex, true, prefix -> getMessageSupplier(prefix, index.id(), index.seqNo(), index.primaryTerm(), index.origin()));
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
        if (LOGGER.isDebugEnabled() || warnLimiter.mayHavePermit()) {
            for (int i = 0; i < results.size(); i++) {
                Engine.IndexResult result = results.get(i);
                if (result.getResultType() == Engine.Result.Type.FAILURE) {
                    int docIndex = i;
                    logFailure(
                        result.getFailure(),
                        // the engine already warned once for the abort, so its repeats must not use up the warn limit
                        !result.isBatchAborted(),
                        prefix -> getMessageSupplier(prefix, batch.id(docIndex), batch.seqNo(docIndex), batch.primaryTerm(), batch.origin())
                    );
                }
            }
        }
    }

    @Override
    public void postIndexBatch(ShardId shardId, IndexOperationBatch batch, Exception ex) {
        logFailure(ex, true, prefix -> getBatchMessageSupplier(prefix, batch));
    }

    private void logFailure(Exception ex, boolean mayWarn, Function<String, Supplier<Object>> messageSupplierForPrefix) {
        if (isUserError(ex)) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(messageSupplierForPrefix.apply("index-fail"), ex);
            }
        } else if (mayWarn && LOGGER.isWarnEnabled() && warnLimiter.tryAcquirePermit()) {
            LOGGER.warn(messageSupplierForPrefix.apply("real-index-fail"), ex);
        } else if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(messageSupplierForPrefix.apply("real-index-fail"), ex);
        }
    }

    private Supplier<Object> getMessageSupplier(
        String messagePrefix,
        String id,
        long seqNo,
        long primaryTerm,
        Engine.Operation.Origin origin
    ) {
        return () -> format(
            "%s [%s] seq# [%s] allocation-id [%s] primaryTerm [%s] operationPrimaryTerm [%s] origin [%s] index [%s] index-mode [%s]",
            messagePrefix,
            id,
            seqNo,
            indexShard.routingEntry().allocationId(),
            primaryTerm,
            indexShard.getOperationPrimaryTerm(),
            origin,
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
            case DocumentMissingException _,VersionConflictEngineException _ -> true;
            case IllegalArgumentException iae -> InternalEngine.isTooManyDocsFailure(iae);
            default -> false;
        };
    }

    private Supplier<Object> getBatchMessageSupplier(String messagePrefix, IndexOperationBatch batch) {
        return () -> format(
            "%s batch docCount [%s] startingSeqNo [%s] allocation-id [%s] primaryTerm [%s] "
                + "operationPrimaryTerm [%s] origin [%s] index [%s] index-mode [%s]",
            messagePrefix,
            batch.docCount(),
            batch.seqNo(0),
            indexShard.routingEntry().allocationId(),
            batch.primaryTerm(),
            indexShard.getOperationPrimaryTerm(),
            batch.origin(),
            indexShard.indexSettings().getIndex().getName(),
            indexShard.indexSettings().getMode()
        );
    }
}
