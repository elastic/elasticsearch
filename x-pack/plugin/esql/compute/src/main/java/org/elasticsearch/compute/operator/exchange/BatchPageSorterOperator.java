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
import org.elasticsearch.compute.operator.Operator;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

/**
 * Operator that reorders BatchPages within each batch to ensure they are output
 * in sequential order based on pageIndexInBatch.
 * <p>
 * Pages may arrive out of order (e.g., due to network ordering), but consumers
 * expect pages within a batch to arrive in order (pageIndexInBatch = 0, 1, 2, ...).
 * This operator buffers out-of-order pages and releases them in the correct sequence.
 * <p>
 * A batch is considered complete when the page with isLastPageInBatch=true has been
 * output (not just received - we must wait for all preceding pages first).
 */
public class BatchPageSorterOperator implements Operator {
    private static final Logger logger = LogManager.getLogger(BatchPageSorterOperator.class);

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

    /** Whether finish() has been called */
    private boolean finished = false;

    @Override
    public boolean needsInput() {
        // Can accept input if not finished
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        if ((page instanceof BatchPage) == false) {
            throw new IllegalArgumentException("BatchPageSorterOperator requires BatchPage input, got: " + page.getClass().getSimpleName());
        }
        BatchPage batchPage = (BatchPage) page;
        long batchId = batchPage.batchId();
        int pageIndex = batchPage.pageIndexInBatch();
        boolean isLast = batchPage.isLastPageInBatch();

        logger.debug(
            "[BatchPageSorterOperator] Received page: batchId={}, pageIndex={}, isLast={}, positions={}",
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
            logger.debug(
                "[BatchPageSorterOperator] Buffering out-of-order page: batchId={}, pageIndex={}, expected={}",
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
            logger.debug("[BatchPageSorterOperator] Batch {} complete, removing state", batchId);
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
                logger.debug(
                    "[BatchPageSorterOperator] Flushed buffered page: batchId={}, pageIndex={}",
                    bufferedPage.batchId(),
                    bufferedPage.pageIndexInBatch()
                );
            } else {
                // Gap in sequence - wait for missing page
                break;
            }
        }
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        // Finished when finish() called AND no more output to produce
        return finished && outputQueue.isEmpty() && hasBufferedPages() == false;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return outputQueue.isEmpty() == false;
    }

    @Override
    public Page getOutput() {
        return outputQueue.poll();
    }

    /**
     * Check if there are any buffered pages waiting for missing pages.
     */
    private boolean hasBufferedPages() {
        for (BatchState state : activeBatches.values()) {
            if (state.bufferedPages.isEmpty() == false) {
                return true;
            }
        }
        return false;
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
    }

    @Override
    public String toString() {
        return "BatchPageSorterOperator[activeBatches=" + activeBatches.size() + ", outputQueueSize=" + outputQueue.size() + "]";
    }
}
