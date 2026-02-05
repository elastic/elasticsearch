/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

import java.io.IOException;

/**
 * Extends Page with batch metadata for bidirectional batch exchange.
 * This allows batch boundaries and batch IDs to travel with pages through exchanges.
 */
public class BatchPage extends Page {
    private final long batchId;
    private final int pageIndexInBatch;
    private final boolean isLastPageInBatch;

    /**
     * Wrap a Page with batch metadata
     */
    public BatchPage(Page page, long batchId, int pageIndexInBatch, boolean isLastPageInBatch) {
        super(page);  // Use protected copy constructor
        this.batchId = batchId;
        this.pageIndexInBatch = pageIndexInBatch;
        this.isLastPageInBatch = isLastPageInBatch;
    }

    /**
     * Create a marker page for batch completion.
     * A marker page is an empty page with isLastPageInBatch=true.
     *
     * @param batchId the batch ID
     * @param pageIndexInBatch the index of this marker within the batch (0 for empty batches,
     *                         or the next index if pages were already sent)
     */
    public static BatchPage createMarker(long batchId, int pageIndexInBatch) {
        return new BatchPage(0, new Block[0], batchId, pageIndexInBatch, true);
    }

    /**
     * Protected constructor for creating empty marker pages
     */
    protected BatchPage(int positionCount, Block[] blocks, long batchId, int pageIndexInBatch, boolean isLastPageInBatch) {
        super(false, positionCount, blocks);
        this.batchId = batchId;
        this.pageIndexInBatch = pageIndexInBatch;
        this.isLastPageInBatch = isLastPageInBatch;
    }

    public long batchId() {
        return batchId;
    }

    public int pageIndexInBatch() {
        return pageIndexInBatch;
    }

    public boolean isLastPageInBatch() {
        return isLastPageInBatch;
    }

    /**
     * Check if this is a marker page (empty page with isLastPageInBatch=true).
     * Marker pages are used to signal batch completion for empty batches.
     */
    public boolean isBatchMarkerOnly() {
        return getPositionCount() == 0 && isLastPageInBatch;
    }

    /**
     * Constructor for deserialization from StreamInput.
     */
    public BatchPage(StreamInput in) throws IOException {
        super(in);  // Call Page constructor to read positionCount and blocks
        this.batchId = in.readLong();
        this.pageIndexInBatch = in.readInt();
        this.isLastPageInBatch = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);  // Write Page data (positionCount and blocks)
        out.writeLong(batchId);
        out.writeInt(pageIndexInBatch);
        out.writeBoolean(isLastPageInBatch);
    }
}
