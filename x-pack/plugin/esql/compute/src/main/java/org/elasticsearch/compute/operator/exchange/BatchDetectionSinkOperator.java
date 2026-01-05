/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SinkOperator;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * Sink operator that detects batch completion and collects result pages.
 * Used by BidirectionalBatchExchangeClient to process batch pages from the server.
 */
public class BatchDetectionSinkOperator extends SinkOperator {
    private static final Logger logger = LogManager.getLogger(BatchDetectionSinkOperator.class);

    private final ExchangeSourceOperator serverToClientSource;
    private final Consumer<BatchPage> resultPageCollector;
    private final BidirectionalBatchExchangeClient.BatchDoneListener batchDoneListener;
    private final LongConsumer updateCompletedBatchId; // Function to update completedBatchId
    private final AtomicReference<Exception> failureRef; // Reference to the failureRef in the client

    public BatchDetectionSinkOperator(
        ExchangeSourceOperator serverToClientSource,
        Consumer<BatchPage> resultPageCollector,
        BidirectionalBatchExchangeClient.BatchDoneListener batchDoneListener,
        LongConsumer updateCompletedBatchId,
        AtomicReference<Exception> failureRef
    ) {
        this.serverToClientSource = serverToClientSource;
        this.resultPageCollector = resultPageCollector;
        this.batchDoneListener = batchDoneListener;
        this.updateCompletedBatchId = updateCompletedBatchId;
        this.failureRef = failureRef;
    }

    @Override
    public boolean needsInput() {
        boolean needs = serverToClientSource == null || serverToClientSource.isFinished() == false;
        logger.debug(
            "[CLIENT] BatchDetectionSinkOperator.needsInput() called: returning={}, sourceFinished={}",
            needs,
            serverToClientSource != null ? serverToClientSource.isFinished() : "null"
        );
        return needs;
    }

    @Override
    protected void doAddInput(Page page) {
        // SinkOperator.doAddInput requires Page parameter, but we know it's always BatchPage
        BatchPage batchPage = (BatchPage) page;
        logger.debug(
            "[CLIENT] BatchDetectionSinkOperator.doAddInput: batchId={}, isLastPageInBatch={}, positionCount={}, isMarker={}",
            batchPage.batchId(),
            batchPage.isLastPageInBatch(),
            batchPage.getPositionCount(),
            batchPage.isBatchMarker()
        );
        try {
            // Only collect BatchPage if it has data (positionCount > 0)
            // Empty BatchPages (markers) are still used for batch completion but not passed to collector
            if (batchPage.getPositionCount() > 0 && resultPageCollector != null) {
                try {
                    // Collector receives the BatchPage - it should copy data if needed
                    resultPageCollector.accept(batchPage);
                } catch (Exception e) {
                    logger.error("[CLIENT][ERROR] Error in result page collector for BatchPage", e);
                }
            }

            // Handle batch completion (even if BatchPage has no data)
            if (batchPage.isLastPageInBatch()) {
                long batchId = batchPage.batchId();
                logger.info("[CLIENT] Received batch completion marker for batchId={}, calling batchDoneListener", batchId);
                if (batchDoneListener != null) {
                    try {
                        batchDoneListener.onBatchDone(batchId);
                        // Track the highest batch ID that has been completed - only after listener callback succeeds
                        updateCompletedBatchId.accept(batchId);
                        logger.info("[CLIENT] Batch done listener called successfully for batchId={}", batchId);
                    } catch (Exception e) {
                        logger.error("[CLIENT][ERROR] Error in batch done listener for batchId=" + batchId, e);
                        failureRef.compareAndSet(null, e);
                        // Don't update completedBatchId if listener failed - batch is not truly completed
                    }
                } else {
                    logger.warn("[CLIENT] Batch done listener is null for batchId={}", batchId);
                    // If listener is null, we can't track completion properly, but update anyway to avoid hanging
                    updateCompletedBatchId.accept(batchId);
                }
            }
        } finally {
            // Release page after processing (collector should have copied data if needed)
            batchPage.releaseBlocks();
        }
    }

    @Override
    public void finish() {
        // No-op
    }

    @Override
    public boolean isFinished() {
        boolean finished = serverToClientSource != null && serverToClientSource.isFinished();
        logger.debug(
            "[CLIENT] BatchDetectionSinkOperator.isFinished() called: returning={}, sourceFinished={}",
            finished,
            serverToClientSource != null ? serverToClientSource.isFinished() : "null"
        );
        return finished;
    }

    @Override
    public IsBlockedResult isBlocked() {
        return Operator.NOT_BLOCKED;
    }

    @Override
    public void close() {
        // No-op - resources are managed by BidirectionalBatchExchangeClient
    }

}
