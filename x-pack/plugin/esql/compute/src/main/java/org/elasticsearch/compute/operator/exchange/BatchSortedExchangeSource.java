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

        // Poll from delegate
        Page page = delegate.pollPage();
        if (page == null) {
            return null;
        }

        // Must be a BatchPage
        if ((page instanceof BatchPage) == false) {
            throw new IllegalArgumentException(
                "BatchSortedExchangeSource requires BatchPage input, got: " + page.getClass().getSimpleName()
            );
        }

        // Add to sorter and return next in-order page
        addToSorter((BatchPage) page);
        return outputQueue.poll();
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
     *
     * @return NOT_BLOCKED if a page is available, otherwise a blocked result
     */
    public IsBlockedResult waitForReading() {
        // If we have pages ready in output queue, not blocked
        if (outputQueue.isEmpty() == false) {
            return NOT_BLOCKED;
        }
        // Otherwise delegate to the underlying source
        return delegate.waitForReading();
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
