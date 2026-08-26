/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Metadata for batch processing in bidirectional batch exchanges.
 * This allows batch boundaries and batch IDs to travel with pages through exchanges.
 *
 * @param batchId the unique identifier for this batch
 * @param pageIndexInBatch the index of this page within the batch (0-based)
 * @param isLastPageInBatch true if this is the last page in the batch
 */
public record BatchMetadata(long batchId, int pageIndexInBatch, boolean isLastPageInBatch) implements Writeable {

    /**
     * Create a marker metadata for batch completion.
     * A marker is used with an empty page (0 positions, 0 blocks) to signal batch completion
     * when a batch produces no output.
     *
     * @param batchId the batch ID
     * @param pageIndexInBatch the index of this marker within the batch (0 for empty batches,
     *                         or the next index if pages were already sent)
     */
    public static BatchMetadata createMarker(long batchId, int pageIndexInBatch) {
        return new BatchMetadata(batchId, pageIndexInBatch, true);
    }

    /**
     * Read BatchMetadata from a stream.
     */
    public static BatchMetadata readFrom(StreamInput in) throws IOException {
        return new BatchMetadata(in.readVLong(), in.readVInt(), in.readBoolean());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(batchId);
        out.writeVInt(pageIndexInBatch);
        out.writeBoolean(isLastPageInBatch);
    }
}
