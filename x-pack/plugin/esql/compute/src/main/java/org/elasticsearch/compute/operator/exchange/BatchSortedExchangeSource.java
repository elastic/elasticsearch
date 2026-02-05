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

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

import static org.elasticsearch.compute.operator.Operator.NOT_BLOCKED;

/**
 * Wraps an {@link ExchangeSource} and reorders BatchPages within each batch
 * to ensure they are output in sequential order based on pageIndexInBatch.
 * <p>
 * Pages may arrive out of order (e.g., due to network ordering), but consumers
 * expect pages within a batch to arrive in order (pageIndexInBatch = 0, 1, 2, ...).
 * This class buffers out-of-order pages and releases them in the correct sequence.
 * <p>
 * Uses composition pattern - wraps an ExchangeSource and provides sorted output.
 */
public class BatchSortedExchangeSource implements Closeable {
    private static final Logger logger = LogManager.getLogger(BatchSortedExchangeSource.class);

    private final ExchangeSource delegate;

    /**
     * Tracks state for a single batch.
     */
    private static class BatchState {
        /** The next pageIndexInBatch we expect to output */
        int nextExpectedIndex = 0;
        /** Pages waiting to be output, keyed by pageIndexInBatch */
        final TreeMap<Integer, BatchPage> bufferedPages = new TreeMap<>();
        /** Index of the last page in batch (-1 if not yet received) */
        int lastPageIndex = -1;

        boolean isComplete() {
            // Complete when we've received the last page marker AND output all pages up to it
            return lastPageIndex >= 0 && nextExpectedIndex > lastPageIndex;
        }
    }

    /** State for each active batch, keyed by batchId */
    private final Map<Long, BatchState> activeBatches = new HashMap<>();

    /** Queue of pages ready to be output (in correct order) */
    private final Queue<BatchPage> outputQueue = new ArrayDeque<>();

    public BatchSortedExchangeSource(ExchangeSource delegate) {
        this.delegate = delegate;
    }

    /**
     * Poll the next page in sorted order.
     * <p>
     * If there are pages ready in the output queue, returns one immediately.
     * Otherwise, polls from the delegate and sorts incoming pages.
     *
     * @return the next page in order, or null if no pages are available
     */
    public BatchPage pollPage() {
        // First, return from output queue if available
        BatchPage ready = outputQueue.poll();
        if (ready != null) {
            return ready;
        }

        // Try to poll and sort from delegate
        pollAndSortFromDelegate();
        return outputQueue.poll();
    }

    /**
     * Polls a page from the delegate and adds it to the sorter.
     * The page may end up in the output queue (if in-order) or be buffered (if out-of-order).
     *
     * @return true if a page was polled from delegate, false if delegate had no pages
     */
    private boolean pollAndSortFromDelegate() {
        Page page = delegate.pollPage();
        if (page == null) {
            return false;
        }

        // Must be a BatchPage
        if ((page instanceof BatchPage) == false) {
            page.releaseBlocks();
            throw new IllegalArgumentException(
                "BatchSortedExchangeSource requires BatchPage input, got: " + page.getClass().getSimpleName()
            );
        }

        addToSorter((BatchPage) page);
        return true;
    }

    /**
     * Add a page to the sorter, potentially releasing pages to the output queue.
     */
    private void addToSorter(BatchPage batchPage) {
        long batchId = batchPage.batchId();
        int pageIndex = batchPage.pageIndexInBatch();
        boolean isLast = batchPage.isLastPageInBatch();

        logger.trace(
            "[BatchSortedExchangeSource] Received page: batchId={}, pageIndex={}, isLast={}, positions={}",
            batchId,
            pageIndex,
            isLast,
            batchPage.getPositionCount()
        );

        // Get or create batch state
        BatchState state = activeBatches.computeIfAbsent(batchId, k -> new BatchState());

        // Record the last page index if this is the last page
        if (isLast) {
            state.lastPageIndex = pageIndex;
        }

        // Check if this is the next expected page
        if (pageIndex == state.nextExpectedIndex) {
            // Output this page immediately
            outputQueue.add(batchPage);
            state.nextExpectedIndex++;

            // Check if any buffered pages can now be output
            flushBufferedPages(state);
        } else if (pageIndex > state.nextExpectedIndex) {
            // Page arrived out of order - buffer it
            logger.trace(
                "[BatchSortedExchangeSource] Buffering out-of-order page: batchId={}, pageIndex={}, expected={}",
                batchId,
                pageIndex,
                state.nextExpectedIndex
            );
            state.bufferedPages.put(pageIndex, batchPage);
        } else {
            // Page index is less than expected - this shouldn't happen (duplicate or invalid)
            batchPage.releaseBlocks();
            throw new IllegalStateException(
                "Received page with unexpected index: batchId="
                    + batchId
                    + ", pageIndex="
                    + pageIndex
                    + ", expected="
                    + state.nextExpectedIndex
            );
        }

        // Clean up completed batches
        if (state.isComplete()) {
            logger.debug("[BatchSortedExchangeSource] Batch {} complete, removing state", batchId);
            activeBatches.remove(batchId);
        }
    }

    /**
     * Flush any buffered pages that are now in sequence.
     */
    private void flushBufferedPages(BatchState state) {
        while (state.bufferedPages.isEmpty() == false) {
            Integer nextKey = state.bufferedPages.firstKey();
            if (nextKey == state.nextExpectedIndex) {
                BatchPage bufferedPage = state.bufferedPages.remove(nextKey);
                outputQueue.add(bufferedPage);
                state.nextExpectedIndex++;
                logger.trace(
                    "[BatchSortedExchangeSource] Flushed buffered page: batchId={}, pageIndex={}",
                    bufferedPage.batchId(),
                    bufferedPage.pageIndexInBatch()
                );
            } else {
                // Gap in sequence - wait for missing page
                break;
            }
        }
    }

    /**
     * Returns an {@link IsBlockedResult} that resolves when a page is available.
     * <p>
     * This method eagerly polls and sorts pages from the delegate until either:
     * <ul>
     *   <li>An in-order page is available in the output queue</li>
     *   <li>The delegate is blocked (returns the delegate's future)</li>
     *   <li>The delegate is finished</li>
     * </ul>
     * This prevents busy-spinning in the driver when pages arrive out of order.
     *
     * @return NOT_BLOCKED if a page is available or delegate is finished, otherwise a blocked result
     */
    public IsBlockedResult waitForReading() {
        // If we have pages ready in output queue, not blocked
        if (outputQueue.isEmpty() == false) {
            return NOT_BLOCKED;
        }

        // Eagerly poll and sort pages from delegate until we have an in-order page
        // or the delegate is blocked/finished
        while (outputQueue.isEmpty()) {
            IsBlockedResult delegateBlocked = delegate.waitForReading();
            if (delegateBlocked.listener().isDone() == false) {
                // Delegate is blocked waiting for more pages
                return delegateBlocked;
            }

            // Delegate has pages or is finished - try to poll and sort
            if (pollAndSortFromDelegate() == false) {
                return NOT_BLOCKED;
            }
        }

        // We have an in-order page ready
        return NOT_BLOCKED;
    }

    /**
     * Blocks until a page is ready in outputQueue OR delegate is finished.
     * Unlike {@link #waitForReading()}, this guarantees {@link #hasReadyPages()} == true
     * when returning NOT_BLOCKED (unless delegate is finished with no more pages).
     * <p>
     * This prevents busy-spinning when pages arrive out of order.
     *
     * @return NOT_BLOCKED if a page is ready or delegate is finished, otherwise a blocked result
     */
    public IsBlockedResult waitUntilReady() {
        if (outputQueue.isEmpty() == false) {
            return NOT_BLOCKED;
        }

        // Keep trying until we have a ready page or delegate is truly finished
        while (outputQueue.isEmpty()) {
            IsBlockedResult delegateBlocked = delegate.waitForReading();
            if (delegateBlocked.listener().isDone() == false) {
                return delegateBlocked; // Block on delegate
            }

            // Delegate says ready - try to poll
            if (pollAndSortFromDelegate() == false) {
                // No page polled - delegate finished or nothing available
                if (delegate.isFinished() && hasNoBufferedPages()) {
                    return NOT_BLOCKED; // Truly finished
                }
                // Pages buffered but not ready - need to wait for more
                // Return delegate's blocked result to wait for more pages
                return delegate.waitForReading();
            }
        }

        return NOT_BLOCKED; // outputQueue has pages
    }

    /**
     * Returns true if there are pages ready to be output (in correct order).
     * This only counts pages in the output queue, not out-of-order pages
     * buffered internally or pages in the delegate's buffer.
     */
    public boolean hasReadyPages() {
        return outputQueue.isEmpty() == false;
    }

    /**
     * Returns true if all pages have been consumed.
     */
    public boolean isFinished() {
        return outputQueue.isEmpty() && hasNoBufferedPages() && delegate.isFinished();
    }

    /**
     * Check if there are any buffered pages waiting for missing pages.
     */
    private boolean hasNoBufferedPages() {
        for (BatchState state : activeBatches.values()) {
            if (state.bufferedPages.isEmpty() == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns the number of pages waiting in the output queue.
     */
    public int bufferSize() {
        return outputQueue.size() + delegate.bufferSize();
    }

    /**
     * Finish the underlying source.
     */
    public void finish() {
        delegate.finish();
    }

    @Override
    public void close() {
        // Release any buffered pages
        for (BatchState state : activeBatches.values()) {
            for (BatchPage page : state.bufferedPages.values()) {
                page.releaseBlocks();
            }
            state.bufferedPages.clear();
        }
        activeBatches.clear();

        // Release any pages in output queue
        BatchPage page;
        while ((page = outputQueue.poll()) != null) {
            page.releaseBlocks();
        }

        // Finish the delegate
        delegate.finish();
    }

    @Override
    public String toString() {
        return "BatchSortedExchangeSource[activeBatches=" + activeBatches.size() + ", outputQueueSize=" + outputQueue.size() + "]";
    }
}
