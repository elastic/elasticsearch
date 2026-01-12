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

    PageToBatchPageOperator(SinkOperator delegate) {
        this.delegate = delegate;
    }

    /**
     * Set the batch context for accessing batch state.
     */
    void setBatchContext(BatchContext batchContext) {
        this.batchContext = batchContext;
    }

    @Override
    public boolean needsInput() {
        return delegate.needsInput();
    }

    @Override
    protected void doAddInput(Page page) {
        if (page instanceof BatchPage) {
            throw new IllegalArgumentException("PageToBatchPageOperator received a BatchPage - this should not happen");
        }

        if (batchContext == null) {
            throw new IllegalStateException("Cannot convert Page to BatchPage: BatchContext not set on PageToBatchPageOperator");
        }

        long batchId = batchContext.getBatchId();
        if (batchId == BatchContext.UNDEFINED_BATCH_ID) {
            throw new IllegalStateException("Cannot convert Page to BatchPage: no batch ID detected yet");
        }

        // If we have a buffered page, send it now (with isLastPageInBatch=false)
        if (bufferedPage != null) {
            sendBufferedPage(false);
        }

        // Buffer the new page
        bufferedPage = page;
        bufferedBatchId = batchId;
    }

    /**
     * Send the buffered page to the delegate sink.
     * @param isLastPageInBatch whether this is the last page in the batch
     */
    private void sendBufferedPage(boolean isLastPageInBatch) {
        if (bufferedPage == null) {
            return;
        }

        BatchPage batchPage = new BatchPage(bufferedPage, bufferedBatchId, isLastPageInBatch);
        // Release the original page - BatchPage now owns the blocks
        bufferedPage.releaseBlocks();
        bufferedPage = null;

        delegate.addInput(batchPage);

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
        if (bufferedPage != null) {
            // Send buffered page with isLastPageInBatch=true
            sendBufferedPage(true);
        } else {
            // No pages were produced - send empty marker page
            logger.debug("[PageToBatchPageOperator] No pages buffered, sending empty marker for batch {}", batchId);
            BatchPage marker = BatchPage.createMarker(batchId);
            delegate.addInput(marker);
        }
        bufferedBatchId = BatchContext.UNDEFINED_BATCH_ID;
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
        // Release any buffered page on close
        if (bufferedPage != null) {
            bufferedPage.releaseBlocks();
            bufferedPage = null;
        }
        delegate.close();
    }
}
