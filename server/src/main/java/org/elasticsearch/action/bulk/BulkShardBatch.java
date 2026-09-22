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
import org.elasticsearch.sourcebatch.SourceRowXContentParser;

import java.io.IOException;
import java.util.List;

public class BulkShardBatch implements Writeable {

    private final SourceBatch batch;
    // Built on first use by the sequential path and shared by every row of the batch.
    private SourceRowXContentParser.SchemaNode schemaTree;

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

    public SourceRowXContentParser.SchemaNode schemaTree() {
        if (schemaTree == null) {
            schemaTree = SourceRowXContentParser.buildSchemaTree(batch.schema());
        }
        return schemaTree;
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
     * Returns true if {@code items} map 1:1 and in order onto {@code batch}'s rows.
     */
    static boolean rowsAlignWithItems(SourceBatch batch, List<BulkItemRequest> items) {
        if (items.size() != batch.docCount()) {
            return false;
        }
        for (int i = 0; i < items.size(); i++) {
            if (items.get(i).request() instanceof IndexRequest indexRequest) {
                if (indexRequest.indexSource().rowIndex() != i) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
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
}
