/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexSource;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.sourcebatch.SourceBatch;

import java.io.IOException;

public class BulkShardBatch implements Writeable {

    private final SourceBatch batch;

    public BulkShardBatch(SourceBatch batch) {
        if (batch == null) {
            throw new IllegalArgumentException("batch must not be null");
        }
        this.batch = batch;
    }

    public BulkShardBatch(StreamInput in) throws IOException {
        this.batch = EscfBatch.parse(in.readBytesReference(), () -> {});
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesReference(batch.data());
    }

    public SourceBatch getBatch() {
        return batch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BulkShardBatch that = (BulkShardBatch) o;
        return batch.data().equals(that.batch.data());
    }

    @Override
    public int hashCode() {
        return batch.data().hashCode();
    }

    /**
     * Wires the given batch into every item's {@link IndexSource} that has a pending row index. This is called on the
     * receiving node after a {@link BulkShardRequest} (and its embedded batch) have been deserialized.
     */
    public static void attachBatchToItems(SourceBatch batch, BulkItemRequest[] items) {
        int rowNumber = 0;
        for (BulkItemRequest item : items) {
            // Only use batch currently when 100% index requests
            IndexRequest indexRequest = (IndexRequest) item.request();
            IndexSource indexSource = indexRequest.indexSource();
            assert indexSource.bytes().length() == 0 : indexSource.bytes().length();
            // TODO: At the moment this is just implicit. However, we may need to eventually add the row serialized directly in the
            // source.
            indexSource.setSourceRow(batch, rowNumber++);
        }
        assert rowNumber == batch.docCount();
    }

    /**
     * For each item converted to an EIRF row, serializes that row back into its original content type and restores it as the
     * inline source, then detaches the batch from the request. No-op if no batch is attached.
     */
    public static void ensureInlineSources(BulkShardRequest request) throws IOException {
        BulkShardBatch shardBatch = request.getBulkShardBatch();
        if (shardBatch == null) {
            return;
        }
        for (BulkItemRequest item : request.items()) {
            IndexRequest indexRequest = (IndexRequest) item.request();
            IndexSource indexSource = indexRequest.indexSource();
            indexSource.ensureInlineSource();
        }
        request.setBulkShardBatch(null);
    }
}
