/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Shared context for batch state management between BatchDriver and PageToBatchPageOperator.
 * <p>
 * Batch lifecycle states:
 * <pre>
 * NOT_STARTED ──► ACTIVE ──► DRAINING ──► IDLE ──► ACTIVE ──► DRAINING ──► IDLE ...
 *                 (first batch)          (subsequent batches)
 * </pre>
 * <p>
 * Thread safety: This class is designed for single-threaded driver execution.
 */
public final class BatchContext {
    private static final Logger logger = LogManager.getLogger(BatchContext.class);
    static final long UNDEFINED_BATCH_ID = -999;

    /**
     * Batch lifecycle states.
     */
    public enum BatchLifecycle {
        /** Initial state - never received any batch */
        NOT_STARTED,
        /** Processing a batch - receiving and processing pages */
        ACTIVE,
        /** Last page received, waiting for pipeline to drain before flush */
        DRAINING,
        /** Between batches, ready for next batch */
        IDLE
    }

    private BatchLifecycle state = BatchLifecycle.NOT_STARTED;
    private long batchId = UNDEFINED_BATCH_ID;

    /**
     * Get the current batch state.
     */
    public BatchLifecycle getState() {
        return state;
    }

    /**
     * Get the current batch ID.
     * @return the current batch ID, or {@link #UNDEFINED_BATCH_ID} if no batch is active
     */
    public long getBatchId() {
        return batchId;
    }

    /**
     * Check if a batch is currently active (ACTIVE or DRAINING).
     */
    public boolean isBatchActive() {
        return state == BatchLifecycle.ACTIVE || state == BatchLifecycle.DRAINING;
    }

    /**
     * Transition to ACTIVE state when first page of a new batch is received.
     * Valid from: NOT_STARTED, IDLE
     * @param newBatchId the batch ID
     */
    void startBatch(long newBatchId) {
        if (state != BatchLifecycle.NOT_STARTED && state != BatchLifecycle.IDLE) {
            throw new IllegalStateException(
                "Cannot start batch " + newBatchId + ": current state is " + state + " (expected NOT_STARTED or IDLE)"
            );
        }
        logger.debug("[BatchContext] {} -> ACTIVE (batch {})", state, newBatchId);
        this.batchId = newBatchId;
        this.state = BatchLifecycle.ACTIVE;
    }

    /**
     * Transition to DRAINING state when last page of batch is received.
     * Valid from: ACTIVE
     */
    void startDraining() {
        if (state != BatchLifecycle.ACTIVE) {
            throw new IllegalStateException("Cannot start draining: current state is " + state + " (expected ACTIVE)");
        }
        logger.debug("[BatchContext] ACTIVE -> DRAINING (batch {})", batchId);
        this.state = BatchLifecycle.DRAINING;
    }

    /**
     * Transition to IDLE state when batch is complete (after flush).
     * Valid from: DRAINING
     */
    void endBatch() {
        if (state != BatchLifecycle.DRAINING) {
            throw new IllegalStateException("Cannot end batch: current state is " + state + " (expected DRAINING)");
        }
        logger.debug("[BatchContext] DRAINING -> IDLE (batch {} complete)", batchId);
        this.batchId = UNDEFINED_BATCH_ID;
        this.state = BatchLifecycle.IDLE;
    }
}
