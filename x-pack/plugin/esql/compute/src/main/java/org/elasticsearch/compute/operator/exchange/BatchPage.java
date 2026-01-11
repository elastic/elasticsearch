/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

/**
 * Extends Page with batch metadata for bidirectional batch exchange.
 * This allows batch boundaries and batch IDs to travel with pages through exchanges.
 */
public class BatchPage extends Page {
    private final long batchId;
    private final boolean isLastPageInBatch;

    /**
     * Wrap a Page with batch metadata
     */
    public BatchPage(Page page, long batchId, boolean isLastPageInBatch) {
        super(page);  // Use protected copy constructor
        this.batchId = batchId;
        this.isLastPageInBatch = isLastPageInBatch;
    }

    /**
     * Create a marker page for empty batches.
     * A marker page is just an empty page with isLastPageInBatch=true.
     */
    public static BatchPage createMarker(long batchId) {
        return new BatchPage(0, new Block[0], batchId, true);
    }

    /**
     * Protected constructor for creating empty marker pages
     */
    protected BatchPage(int positionCount, Block[] blocks, long batchId, boolean isLastPageInBatch) {
        super(false, positionCount, blocks);
        this.batchId = batchId;
        this.isLastPageInBatch = isLastPageInBatch;
    }

    public long batchId() {
        return batchId;
    }

    public boolean isLastPageInBatch() {
        return isLastPageInBatch;
    }

    /**
     * Check if this is a marker page (empty page with isLastPageInBatch=true).
     * Marker pages are used to signal batch completion for empty batches.
     */
    public boolean isBatchMarker() {
        return getPositionCount() == 0 && isLastPageInBatch;
    }

    /**
     * Extract the underlying Page (which is this, since BatchPage IS a Page)
     * Provided for convenience/consistency with wrapper patterns
     */
    public Page asPage() {
        return this;
    }
}
