/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexSource;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.eirf.EirfBatch;
import org.elasticsearch.eirf.EirfEncoder;

import java.io.IOException;

public class BulkShardBatch implements Writeable {

    private final EirfBatch eirfBatch;

    public BulkShardBatch(EirfBatch eirfBatch) {
        if (eirfBatch == null) {
            throw new IllegalArgumentException("eirfBatch must not be null");
        }
        this.eirfBatch = eirfBatch;
    }

    public BulkShardBatch(StreamInput in) throws IOException {
        this.eirfBatch = new EirfBatch(in.readBytesReference(), () -> {});
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesReference(eirfBatch.data());
    }

    public EirfBatch getEirfBatch() {
        return eirfBatch;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BulkShardBatch that = (BulkShardBatch) o;
        return eirfBatch.data().equals(that.eirfBatch.data());
    }

    @Override
    public int hashCode() {
        return eirfBatch.data().hashCode();
    }

    /**
     * Whether the given shard-level request is eligible for conversion to an EIRF batch. Requires every item to be an
     * {@link IndexRequest} with inline source bytes and a known content type, and the request not to be a simulation.
     */
    public static boolean shouldConvertToShardBatch(BulkShardRequest bulkShardRequest) {
        if (bulkShardRequest.isSimulated()) {
            return false;
        }
        final BulkItemRequest[] items = bulkShardRequest.items();
        if (items.length == 0) {
            return false;
        }
        for (BulkItemRequest item : items) {
            final DocWriteRequest<?> request = item.request();
            if (request instanceof IndexRequest indexRequest) {
                if (indexRequest.indexSource().hasSource() == false || indexRequest.getContentType() == null) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * Encodes the items of the given {@link BulkShardRequest} into an EIRF batch, replaces each item's inline source with a row
     * reference via {@link IndexSource#setEirfRow(EirfBatch, int)}, and attaches the batch to the request.
     */
    public static BulkShardBatch createShardBatch(BulkShardRequest bulkShardRequest) throws IOException {
        BulkItemRequest[] items = bulkShardRequest.items();
        EirfBatch batch;
        // TODO: Pooled eventually
        try (EirfEncoder encoder = new EirfEncoder()) {
            for (BulkItemRequest item : items) {
                IndexRequest indexRequest = (IndexRequest) item.request();
                encoder.addDocument(indexRequest.indexSource().bytes(), indexRequest.getContentType());
            }
            batch = encoder.build();
        }
        for (int i = 0; i < items.length; i++) {
            IndexRequest indexRequest = (IndexRequest) items[i].request();
            indexRequest.indexSource().setEirfRow(batch, i);
        }
        return new BulkShardBatch(batch);
    }

    /**
     * Wires the given batch into every item's {@link IndexSource} that has a pending EIRF row index. This is called on the
     * receiving node after a {@link BulkShardRequest} (and its embedded batch) have been deserialized.
     */
    public static void attachBatchToItems(EirfBatch batch, BulkItemRequest[] items) {
        int rowNumber = 0;
        for (BulkItemRequest item : items) {
            // Only use batch currently when 100% index requests
            IndexRequest indexRequest = (IndexRequest) item.request();
            IndexSource indexSource = indexRequest.indexSource();
            assert indexSource.bytes().length() == 0 : indexSource.bytes().length();
            // TODO: At the moment this is just implicit. However, we may need to eventually add the row serialized directly in the
            // source.
            indexSource.setEirfRow(batch, rowNumber++);
        }
        assert rowNumber == batch.docCount();
    }

    /**
     * Reverses {@link #createShardBatch(BulkShardRequest)}: for each item that was converted to an EIRF row, serializes that row back
     * into its original content type and restores it as the inline source, then detaches the batch from the request. No-op if no
     * batch is attached.
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
