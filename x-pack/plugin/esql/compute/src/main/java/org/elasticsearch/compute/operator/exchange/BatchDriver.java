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

    private final AtomicLong detectedBatchIdRef;

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
        ExchangeSourceOperator source,
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
        this.detectedBatchIdRef = new AtomicLong(UNDEFINED_BATCH_ID);

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

        // Set driver on PageToBatchPageOperator if the sink is wrapped
        if (sink instanceof PageToBatchPageOperator wrappedSink) {
            wrappedSink.setDriver(this);
        }
    }

    private static SourceOperator wrapSource(ExchangeSourceOperator source) {
        // Wrap source to detect BatchPage with isLastPageInBatch=true and extract batchId
        return new WrappedSourceOperator(source);
    }

    /**
     * Wraps a sink operator to convert Pages to BatchPages before sending to the sink.
     * This ensures all output pages have batch metadata attached.
     * The driver reference must be set after BatchDriver construction via setDriver().
     */
    static PageToBatchPageOperator wrapSink(SinkOperator sink) {
        return new PageToBatchPageOperator(sink);
    }

    private static class WrappedSourceOperator extends SourceOperator {
        private final ExchangeSourceOperator delegate;
        private BatchDriver driver;

        WrappedSourceOperator(ExchangeSourceOperator delegate) {
            this.delegate = delegate;
        }

        void setDriver(BatchDriver driver) {
            this.driver = driver;
        }

        @Override
        public Page getOutput() {
            // If batch has ended or callback is in progress, don't poll new pages
            // This prevents pages from the next batch from being polled before the current batch completes
            if (driver.batchEnded || driver.callbackInProgress) {
                return null;
            }

            Page page = delegate.getOutput();
            if (page != null) {
                // Mark that we've received the first page
                if (driver.firstBatchStarted == false) {
                    driver.firstBatchStarted = true;
                }

                // Only accept BatchPage - throw if not
                if ((page instanceof BatchPage) == false) {
                    page.releaseBlocks();
                    throw new IllegalArgumentException(
                        Strings.format("BatchDriver only accepts BatchPage, but received: %s", page.getClass().getName())
                    );
                }

                BatchPage batchPage = (BatchPage) page;
                long pageBatchId = batchPage.batchId();
                long currentBatchId = driver.detectedBatchIdRef.get();

                // Handle batch ID logic
                if (currentBatchId == UNDEFINED_BATCH_ID) {
                    // No current batch - set to new batch ID
                    driver.detectedBatchIdRef.set(pageBatchId);
                } else if (pageBatchId != currentBatchId) {
                    // Got a page for a different batch - this is an error
                    batchPage.releaseBlocks();
                    throw new IllegalStateException(
                        Strings.format("Received page for batch %d but currently processing batch %d", pageBatchId, currentBatchId)
                    );
                }

                if (batchPage.isLastPageInBatch()) {
                    // This is the last page in batch - mark batch as ended
                    driver.batchEnded = true;
                }

                // Unwrap BatchPage to regular Page - extract blocks and create new Page
                // This ensures other operators don't need to handle BatchPage
                page = driver.unwrapBatchPage(batchPage);
            }
            return page;
        }

        @Override
        public boolean isFinished() {
            // Delegate to the source - it correctly reports finished only when:
            // 1. The source is explicitly finished, OR
            // 2. The buffer is finished (client called finish() and buffer is empty)
            // The source should NOT report as finished when it's empty and waiting for pages.
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
            // If batch has ended or callback is in progress, we're blocking new pages
            // So we can't produce more data even if there are buffered pages
            if (driver.batchEnded || driver.callbackInProgress) {
                return false;
            }
            // Delegate to the underlying ExchangeSourceOperator to check if it can produce more data
            // This will check if there are buffered pages that need processing
            return delegate.canProduceMoreDataWithoutExtraInput();
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
     * Note: No synchronization needed - driver runs single-threaded.
     */
    @Override
    protected void onNoPagesMoved() {
        if (batchEnded == false) {
            logger.debug("[SERVER] Callback onNoPagesMoved but batch not ended yet - continuing");
            return; // Batch not ended yet
        }

        if (callbackInProgress) {
            logger.debug("[SERVER] Callback onNoPagesMoved but callback already in progress - waiting");
            return; // Callback already in progress
        }

        // Check if any operator can produce more data without extra input
        // If so, don't declare batch complete yet - there may be buffered data to process
        for (Operator operator : activeOperators) {
            if (operator.canProduceMoreDataWithoutExtraInput()) {
                logger.debug(
                    "[SERVER] BatchDriver: Operator {} can produce more data without extra input - not declaring batch complete yet",
                    operator
                );
                return; // Wait for operator to produce more data
            }
        }

        // No pages moved and batch has ended - check if source is blocked or finished
        if (activeOperators.isEmpty()) {
            // Driver finished - all pages processed, batch is complete
            completeBatch("driver finished, no pages moving");
            return;
        }

        Operator sourceOp = activeOperators.get(0);
        if (sourceOp instanceof SourceOperator source) {
            // When batchEnded is true, getOutput() returns null (blocking new pages)
            // So if batchEnded and no pages moved, the batch is complete
            // Check if source is finished or blocked
            if (source.isFinished() || source.isBlocked().listener().isDone() == false) {
                // Batch is complete - no pages moving and source is blocked/finished
                completeBatch("no pages moved, source blocked/finished");
            } else {
                // Batch ended and source is not blocked/finished
                // Since batchEnded is true, getOutput() will return null (blocking new pages)
                // and no pages are moving, so the batch is complete
                completeBatch("no pages moved, batch ended");
            }
        }
    }

    /**
     * Complete the current batch by calling the callback.
     * Extracted to avoid code duplication.
     */
    private void completeBatch(String reason) {
        long completedBatchId = detectedBatchIdRef.get();
        logger.debug("[SERVER] BatchDriver: Batch {} is complete ({})", completedBatchId, reason);
        callBatchDoneCallback(completedBatchId);
    }

    /**
     * Reset batch state to allow processing the next batch.
     */
    private void resetBatchState() {
        callbackInProgress = false;
        batchEnded = false;
        detectedBatchIdRef.set(UNDEFINED_BATCH_ID);
    }

    /**
     * Unwrap BatchPage to regular Page, handling marker pages and empty pages.
     */
    private Page unwrapBatchPage(BatchPage batchPage) {
        // Skip unwrapping for marker pages (empty pages used to signal batch completion)
        if (batchPage.isBatchMarker()) {
            // Marker page - release it and return null (no data to process)
            batchPage.releaseBlocks();
            return null;
        }
        if (batchPage.getBlockCount() > 0) {
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
            return unwrappedPage;
        }
        // Empty BatchPage (shouldn't happen, but handle it safely)
        batchPage.releaseBlocks();
        return null;
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
        logger.debug("[SERVER] BatchDriver: Calling batch done callback for batchId={}", batchId);

        // Call all registered listeners
        if (batchId == UNDEFINED_BATCH_ID) {
            logger.warn("[SERVER] BatchDriver: No batchId detected, using {}", UNDEFINED_BATCH_ID);
        }
        batchDoneNotifier.notifyBatchDone(batchId);

        // Reset state after callback completes
        // Reset both batchEnded and callbackInProgress to allow the next batch
        // Reset batchId to UNDEFINED_BATCH_ID to indicate we're ready for a new batch
        resetBatchState();
        logger.debug("[SERVER] BatchDriver: Batch done callback completed, ready for next batch");
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

    /**
     * Operator that wraps a sink and converts Pages to BatchPages before passing them to the sink.
     * This ensures all output pages have batch metadata attached.
     */
    static class PageToBatchPageOperator extends SinkOperator {
        private final SinkOperator delegate;
        private BatchDriver driver;

        PageToBatchPageOperator(SinkOperator delegate) {
            this.delegate = delegate;
        }

        void setDriver(BatchDriver driver) {
            this.driver = driver;
        }

        @Override
        public boolean needsInput() {
            return delegate.needsInput();
        }

        @Override
        protected void doAddInput(Page page) {
            // If page is already a BatchPage, pass it through directly
            if (page instanceof BatchPage) {
                delegate.addInput(page);
                return;
            }

            // Convert Page to BatchPage only if it's not already a BatchPage
            if (driver == null) {
                throw new IllegalStateException("Cannot convert Page to BatchPage: driver not set on PageToBatchPageOperator");
            }

            long batchId = driver.detectedBatchIdRef.get();
            if (batchId == UNDEFINED_BATCH_ID) {
                throw new IllegalStateException("Cannot convert Page to BatchPage: no batch ID detected yet");
            }

            // Convert Page to BatchPage
            // Mark as last page in batch if this is the last page before batch ends
            // For now, we'll mark all pages as not last, and let the marker page handle batch completion
            boolean isLastPageInBatch = false;
            BatchPage batchPage = new BatchPage(page, batchId, isLastPageInBatch);

            // Pass BatchPage to delegate sink
            delegate.addInput(batchPage);
        }

        @Override
        public void finish() {
            delegate.finish();
        }

        @Override
        public boolean isFinished() {
            return delegate.isFinished();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
