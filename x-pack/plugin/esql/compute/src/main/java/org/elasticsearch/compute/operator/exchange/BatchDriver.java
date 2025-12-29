/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SinkOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Driver that processes batches on the server side of a BidirectionalBatchExchange.
 * Extends Driver to handle batch-specific logic.
 *
 * <p>This driver:
 * <ul>
 *   <li>Reads pages from the client-to-server exchange</li>
 *   <li>Executes operators (from factories) on those pages</li>
 *   <li>Writes results to the server-to-client exchange</li>
 *   <li>Detects batchId from BatchPage with isLastPageInBatch=true</li>
 *   <li>Tracks batch completion via callback</li>
 * </ul>
 */
public final class BatchDriver extends Driver {
    private static final Logger logger = LogManager.getLogger(BatchDriver.class);
    private static final long UNDEFINED_BATCH_ID = -999;

    private final AtomicLong detectedBatchIdRef = new AtomicLong(UNDEFINED_BATCH_ID);
    private final BatchDoneNotifier batchDoneNotifier = new BatchDoneNotifier();
    private volatile boolean batchEnded = false; // Set to true when isLastPageInBatch page is received
    private volatile boolean callbackInProgress = false; // Set to true when callback is being called
    private volatile boolean firstBatchStarted = false; // Set to true when first page input is received

    public BatchDriver(
        String sessionId,
        String shortDescription,
        String clusterName,
        String nodeName,
        long startTime,
        long startNanos,
        DriverContext driverContext,
        Supplier<String> description,
        SourceOperator source,
        List<Operator> intermediateOperators,
        SinkOperator sink,
        TimeValue statusInterval,
        Releasable releasable
    ) {
        super(
            sessionId,
            shortDescription,
            clusterName,
            nodeName,
            startTime,
            startNanos,
            driverContext,
            description,
            wrapSource(source),
            intermediateOperators,
            sink,
            statusInterval,
            releasable
        );
        // Set driver reference after super() call
        // We require the first operator to be a WrappedSourceOperator - throw if it's not
        if (activeOperators.isEmpty()) {
            throw new IllegalStateException("BatchDriver requires at least one operator (source operator)");
        }
        Operator firstOperator = activeOperators.get(0);
        if (firstOperator instanceof WrappedSourceOperator wrapped) {
            wrapped.setDriver(this);
        } else {
            throw new IllegalStateException(
                "BatchDriver requires the first operator to be a WrappedSourceOperator, but got: " + firstOperator.getClass().getName()
            );
        }
    }

    private static SourceOperator wrapSource(SourceOperator source) {
        // Wrap source to detect BatchPage with isLastPageInBatch=true and extract batchId
        return new WrappedSourceOperator(source);
    }

    private static class WrappedSourceOperator extends SourceOperator {
        private final SourceOperator delegate;
        private BatchDriver driver;

        WrappedSourceOperator(SourceOperator delegate) {
            this.delegate = delegate;
        }

        void setDriver(BatchDriver driver) {
            this.driver = driver;
        }

        @Override
        public Page getOutput() {
            // Block new pages if batch has ended and callback is in progress
            if (driver != null && driver.batchEnded && driver.callbackInProgress) {
                logger.debug("[SERVER] BatchDriver: Batch ended, blocking new pages until callback completes");
                return null;
            }

            Page page = delegate.getOutput();
            if (page != null) {
                // Mark that we've received the first page
                if (driver != null && driver.firstBatchStarted == false) {
                    synchronized (driver) {
                        if (driver.firstBatchStarted == false) {
                            driver.firstBatchStarted = true;
                            logger.debug("[SERVER] BatchDriver: First batch started - first page received");
                        }
                    }
                }

                // Unwrap BatchPage to regular Page - other operators may not handle BatchPage
                if (page instanceof BatchPage batchPage) {
                    long pageBatchId = batchPage.batchId();
                    if (driver != null) {
                        long currentBatchId = driver.detectedBatchIdRef.get();
                        // If we have a current batch ID and this page is from a different batch,
                        // and we're still processing the current batch or in callback, throw an exception
                        if (currentBatchId != UNDEFINED_BATCH_ID && pageBatchId != currentBatchId) {
                            synchronized (driver) {
                                if (driver.batchEnded || driver.callbackInProgress) {
                                    throw new IllegalStateException(
                                        Strings.format(
                                            "Received page for batch %d while still processing batch %d "
                                                + "(batchEnded=%s, callbackInProgress=%s)",
                                            pageBatchId,
                                            currentBatchId,
                                            driver.batchEnded,
                                            driver.callbackInProgress
                                        )
                                    );
                                }
                            }
                        }
                    }

                    if (batchPage.isLastPageInBatch()) {
                        // This is the last page in batch - extract batchId and mark batch as ended
                        long batchId = batchPage.batchId();
                        if (driver != null) {
                            driver.detectedBatchIdRef.set(batchId);
                            synchronized (driver) {
                                driver.batchEnded = true;
                            }
                            logger.info("[SERVER] BatchDriver: Detected batchId={} from last page in batch, blocking new input", batchId);
                        }
                    }

                    // Unwrap BatchPage to regular Page - extract blocks and create new Page
                    // This ensures other operators don't need to handle BatchPage
                    // Skip unwrapping for marker pages (empty pages used to signal batch completion)
                    if (batchPage.isBatchMarker()) {
                        // Marker page - release it and return null (no data to process)
                        batchPage.releaseBlocks();
                        page = null;
                    } else if (batchPage.getBlockCount() > 0) {
                        // Regular BatchPage with blocks - unwrap it
                        Block[] blocks = new Block[batchPage.getBlockCount()];
                        for (int i = 0; i < blocks.length; i++) {
                            blocks[i] = batchPage.getBlock(i);
                            // Increment ref count for each block since we're sharing them with the new Page
                            blocks[i].incRef();
                        }
                        // Create a new regular Page with the same blocks
                        Page unwrappedPage = new Page(blocks);
                        // Release the BatchPage - blocks are safe because we've incremented their ref count
                        batchPage.releaseBlocks();
                        page = unwrappedPage;
                    } else {
                        // Empty BatchPage (shouldn't happen, but handle it safely)
                        batchPage.releaseBlocks();
                        page = null;
                    }
                }
            }
            return page;
        }

        @Override
        public boolean isFinished() {
            return delegate.isFinished();
        }

        @Override
        public void finish() {
            delegate.finish();
        }

        @Override
        public IsBlockedResult isBlocked() {
            return delegate.isBlocked();
        }

        @Override
        public boolean canProduceMoreDataWithoutExtraInput() {
            // return false, as it does not hold any buffered data
            return false;
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    /**
     * Get the detected batch ID. Returns -1 if not yet detected.
     */
    public long getBatchId() {
        return detectedBatchIdRef.get();
    }

    /**
     * Get the notifier that can be used to subscribe to batch completion events.
     * Multiple batches can complete, and each completion will notify all registered listeners.
     *
     * @return the batch done notifier
     */
    public BatchDoneNotifier onBatchDone() {
        return batchDoneNotifier;
    }

    /**
     * Override onNoPagesMoved() to detect batch completion and call callback.
     * This is called by the base Driver class when movedPage == false.
     */
    @Override
    protected synchronized void onNoPagesMoved() {
        if (batchEnded == false) {
            logger.info("[SERVER] Callback onNoPagesMoved but batch not ended yet - continuing");
            return; // Batch not ended yet
        }

        if (callbackInProgress) {
            logger.info("[SERVER] Callback onNoPagesMoved but callback already in progress - waiting");
            return; // Callback already in progress
        }

        // Check if any operator can produce more data without extra input
        // If so, don't declare batch complete yet - there may be buffered data to process
        for (Operator operator : activeOperators) {
            if (operator.canProduceMoreDataWithoutExtraInput()) {
                logger.info(
                    "[SERVER] BatchDriver: Operator {} can produce more data without extra input - not declaring batch complete yet",
                    operator
                );
                return; // Wait for operator to produce more data
            }
        }

        // No pages moved and batch has ended - check if source is blocked or finished
        if (activeOperators.isEmpty()) {
            // Driver finished - all pages processed, batch is complete
            long completedBatchId = detectedBatchIdRef.get();
            logger.info("[SERVER] BatchDriver: Batch {} is complete (driver finished, no pages moving)", completedBatchId);
            callBatchDoneCallback(completedBatchId);
            return;
        }

        Operator sourceOp = activeOperators.get(0);
        if (sourceOp instanceof SourceOperator source) {
            // When batchEnded is true, getOutput() returns null (blocking new pages)
            // So if batchEnded and no pages moved, the batch is complete
            // Check if source is finished or blocked
            if (source.isFinished() || source.isBlocked().listener().isDone() == false) {
                // Batch is complete - no pages moving and source is blocked/finished
                long completedBatchId = detectedBatchIdRef.get();
                logger.info("[SERVER] BatchDriver: Batch {} is complete (no pages moved, source blocked/finished)", completedBatchId);
                callBatchDoneCallback(completedBatchId);
            } else if (batchEnded) {
                // Batch ended and source is not blocked/finished
                // Since batchEnded is true, getOutput() will return null (blocking new pages)
                // and no pages are moving, so the batch is complete
                long completedBatchId = detectedBatchIdRef.get();
                logger.info("[SERVER] BatchDriver: Batch {} is complete (no pages moved, batch ended)", completedBatchId);
                callBatchDoneCallback(completedBatchId);
            }
        }
    }

    /**
     * Call the batch done callback and reset state after callback completes.
     */
    private void callBatchDoneCallback(long batchId) {
        // Do not call callback unless first batch has started
        if (firstBatchStarted == false) {
            logger.debug("[SERVER] BatchDriver: Skipping callback - first batch not started yet");
            return;
        }

        callbackInProgress = true;
        logger.info("[SERVER] BatchDriver: Calling batch done callback for batchId={}", batchId);

        // Call all registered listeners
        long finalBatchId = batchId != UNDEFINED_BATCH_ID ? batchId : UNDEFINED_BATCH_ID;
        if (batchId == UNDEFINED_BATCH_ID) {
            logger.warn("[SERVER] BatchDriver: No batchId detected, using {}", UNDEFINED_BATCH_ID);
        }
        batchDoneNotifier.notifyBatchDone(finalBatchId);

        // Reset state after callback completes
        // Note: We can't wait for the callback to complete here, but the callback listener
        // should handle resetting batchEnded and callbackInProgress when it's done
        // For now, we'll reset immediately since the callback is fire-and-forget
        // If we need to wait for callback completion, we'd need to track that separately
        batchEnded = false;
        callbackInProgress = false;
        logger.info("[SERVER] BatchDriver: Batch done callback completed, allowing new pages");
    }

    /**
     * Notifier that supports multiple batch completion callbacks.
     * Unlike SubscribableListener which can only be completed once, this allows
     * multiple batch completions to be notified to all registered listeners.
     */
    public static class BatchDoneNotifier {
        private final List<ActionListener<Long>> listeners = new CopyOnWriteArrayList<>();

        /**
         * Add a listener that will be notified when each batch completes.
         * The listener will be called once for each batch that completes.
         */
        public void addListener(ActionListener<Long> listener) {
            listeners.add(listener);
        }

        /**
         * Notify all registered listeners that a batch has completed.
         * This can be called multiple times for different batches.
         */
        void notifyBatchDone(long batchId) {
            for (ActionListener<Long> listener : listeners) {
                try {
                    listener.onResponse(batchId);
                } catch (Exception e) {
                    BatchDriver.logger.error("[SERVER] BatchDriver: Error notifying batch done listener", e);
                    throw new RuntimeException("Failed to notify batch done listener", e);
                }
            }
        }
    }

}
