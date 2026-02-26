/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.compute.data.BatchMetadata;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SinkOperator;

/**
 * Operator that wraps a sink and converts Pages to BatchPages before passing them to the sink.
 * This ensures all output pages have batch metadata attached.
 * <p>
 * Implements single-page buffering to optimize batch marker handling:
 * <ul>
 *   <li>Buffers one page at a time</li>
 *   <li>When a new page arrives, sends the buffered page with isLastPageInBatch=false and buffers the new one</li>
 *   <li>When batch ends, {@link #flushBatch()} sends the buffered page with isLastPageInBatch=true</li>
 *   <li>If no pages were produced (empty result), sends an empty marker page</li>
 * </ul>
 */
final class PageToBatchPageOperator extends SinkOperator {
    private static final Logger logger = LogManager.getLogger(PageToBatchPageOperator.class);

    private final SinkOperator delegate;
    private BatchContext batchContext;
    private Page bufferedPage;
    private long bufferedBatchId = BatchContext.UNDEFINED_BATCH_ID;
    private int nextPageIndexInBatch = 0;
    private boolean flushedInFinish = false;

    PageToBatchPageOperator(SinkOperator delegate) {
        this.delegate = delegate;
        logger.debug("[PageToBatchPageOperator] Created with delegate: {}", delegate.getClass().getSimpleName());
    }

    /**
     * Set the batch context for accessing batch state.
     */
    void setBatchContext(BatchContext batchContext) {
        this.batchContext = batchContext;
    }

    @Override
    public boolean needsInput() {
        boolean result = delegate.needsInput();
        logger.trace("[PageToBatchPageOperator] needsInput() = {}, hasBufferedPage={}", result, bufferedPage != null);
        return result;
    }

    @Override
    protected void doAddInput(Page page) {
        logger.trace(
            "[PageToBatchPageOperator] doAddInput called: pagePositions={}, hasExistingBuffer={}",
            page.getPositionCount(),
            bufferedPage != null
        );

        if (page.batchMetadata() != null) {
            throw new IllegalArgumentException("PageToBatchPageOperator received a page with BatchMetadata - this should not happen");
        }

        if (batchContext == null) {
            throw new IllegalStateException("Cannot add BatchMetadata to Page: BatchContext not set on PageToBatchPageOperator");
        }

        long batchId = batchContext.getBatchId();
        if (batchId == BatchContext.UNDEFINED_BATCH_ID) {
            throw new IllegalStateException("Cannot add BatchMetadata to Page: no batch ID detected yet");
        }

        // If we have a buffered page, send it now (with isLastPageInBatch=false)
        if (bufferedPage != null) {
            logger.trace("[PageToBatchPageOperator] Sending previously buffered page before buffering new one");
            sendBufferedPage(false);
        }

        // Buffer the new page
        bufferedPage = page;
        bufferedBatchId = batchId;
        logger.trace("[PageToBatchPageOperator] Buffered new page for batchId={}", batchId);
    }

    /**
     * Send the buffered page to the delegate sink with batch metadata attached.
     * @param isLastPageInBatch whether this is the last page in the batch
     */
    private void sendBufferedPage(boolean isLastPageInBatch) {
        if (bufferedPage == null) {
            return;
        }

        int pageIndex = nextPageIndexInBatch++;
        BatchMetadata metadata = new BatchMetadata(bufferedBatchId, pageIndex, isLastPageInBatch);
        Page pageWithMetadata = bufferedPage.withBatchMetadata(metadata);
        // Release the original page - pageWithMetadata now owns the blocks (ref count was incremented)
        bufferedPage.releaseBlocks();
        bufferedPage = null;

        delegate.addInput(pageWithMetadata);

        if (isLastPageInBatch) {
            logger.debug("[PageToBatchPageOperator] Sent last page in batch {} with isLastPageInBatch=true", bufferedBatchId);
        }
    }

    /**
     * Flush the batch by sending the buffered page with isLastPageInBatch=true.
     * If no pages were buffered (empty result), sends an empty marker page.
     * Called by BatchDriver when a batch completes.
     */
    void flushBatch() {
        long batchId = batchContext != null ? batchContext.getBatchId() : bufferedBatchId;
        logger.debug(
            "[PageToBatchPageOperator] flushBatch called: batchId={}, hasBufferedPage={}, flushedInFinish={}",
            batchId,
            bufferedPage != null,
            flushedInFinish
        );

        if (flushedInFinish) {
            // Already flushed in finish(), just reset state
            logger.debug("[PageToBatchPageOperator] Already flushed in finish(), skipping duplicate flush");
            flushedInFinish = false;
            bufferedBatchId = BatchContext.UNDEFINED_BATCH_ID;
            nextPageIndexInBatch = 0;
            return;
        }

        if (bufferedPage != null) {
            // Send buffered page with isLastPageInBatch=true
            sendBufferedPage(true);
        } else {
            // No pages were produced - send empty marker page
            logger.debug("[PageToBatchPageOperator] Sending empty marker for batch {}", batchId);
            Page marker = Page.createBatchMarkerPage(batchId, nextPageIndexInBatch);
            delegate.addInput(marker);
            logger.debug("[PageToBatchPageOperator] Empty marker sent for batch {}", batchId);
        }
        bufferedBatchId = BatchContext.UNDEFINED_BATCH_ID;
        nextPageIndexInBatch = 0;
    }

    @Override
    public void finish() {
        logger.debug("[PageToBatchPageOperator] finish() called, hasBufferedPage={}", bufferedPage != null);
        if (bufferedPage != null) {
            logger.debug("[PageToBatchPageOperator] Flushing buffered page in finish()");
            sendBufferedPage(true);
            flushedInFinish = true;
        }
        delegate.finish();
    }

    @Override
    public boolean isFinished() {
        boolean result = delegate.isFinished();
        logger.trace("[PageToBatchPageOperator] isFinished() = {}, hasBufferedPage={}", result, bufferedPage != null);
        return result;
    }

    @Override
    public void close() {
        logger.debug("[PageToBatchPageOperator] close() called, hasBufferedPage={}", bufferedPage != null);
        // Release any buffered page on close
        if (bufferedPage != null) {
            logger.warn("[PageToBatchPageOperator] Releasing buffered page on close - this page was never flushed!");
            bufferedPage.releaseBlocks();
            bufferedPage = null;
        }
        delegate.close();
        logger.debug("[PageToBatchPageOperator] close() completed");
    }
}
