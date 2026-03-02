/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds {@link DocumentBatch} objects from pre-parsed {@link FlattenedDoc} data,
 * avoiding the need to re-parse JSON source. Uses the same serialization logic as
 * {@link DocumentBatchEncoder} but populates {@link DocumentBatchEncoder.ColumnBuilder}
 * from buffered {@link FieldValue} data instead of parsing JSON.
 */
final class SinglePassBatchBuilder {

    private SinglePassBatchBuilder() {}

    /**
     * Build {@link DocumentBatch} objects for each shard from pre-parsed flat docs.
     * Performs post-grouping eligibility checks (>= 2 items, no duplicate IDs).
     * Shards that fail these checks are omitted from the result — the caller should
     * fall back to the existing {@link DocumentBatchEncoder#encode} path for those shards.
     *
     * @param perShardFlatDocs map from shard to the flattened docs assigned to that shard
     * @param requestsByShard  map from shard to bulk item requests (used for size check)
     * @return map from shard to pre-built DocumentBatch (only for eligible shards)
     */
    static Map<ShardId, DocumentBatch> buildBatches(
        Map<ShardId, List<FlattenedDoc>> perShardFlatDocs,
        Map<ShardId, List<BulkItemRequest>> requestsByShard
    ) throws IOException {
        Map<ShardId, DocumentBatch> result = new HashMap<>();
        for (Map.Entry<ShardId, List<FlattenedDoc>> entry : perShardFlatDocs.entrySet()) {
            ShardId shardId = entry.getKey();
            List<FlattenedDoc> flatDocs = entry.getValue();

            // Post-grouping eligibility: need >= 2 docs
            if (flatDocs.size() < 2) {
                continue;
            }

            // Check for duplicate IDs
            if (hasDuplicateIds(flatDocs)) {
                continue;
            }

            // Check that this shard wasn't partially populated by non-single-pass requests
            List<BulkItemRequest> shardRequests = requestsByShard.get(shardId);
            if (shardRequests != null && shardRequests.size() != flatDocs.size()) {
                // Shard has a mix of single-pass and non-single-pass requests; skip pre-building
                continue;
            }

            result.put(shardId, buildBatch(flatDocs));
        }
        return result;
    }

    private static boolean hasDuplicateIds(List<FlattenedDoc> flatDocs) {
        Set<String> seenIds = new HashSet<>();
        for (FlattenedDoc doc : flatDocs) {
            String id = doc.request().id();
            if (id != null && seenIds.add(id) == false) {
                return true;
            }
        }
        return false;
    }

    /**
     * Build a single {@link DocumentBatch} from a list of flattened docs.
     */
    static DocumentBatch buildBatch(List<FlattenedDoc> flatDocs) throws IOException {
        int docCount = flatDocs.size();
        Map<String, DocumentBatchEncoder.ColumnBuilder> columns = new LinkedHashMap<>();

        // Phase 1: Replay field values into column builders
        for (int docIdx = 0; docIdx < docCount; docIdx++) {
            FlattenedDoc flatDoc = flatDocs.get(docIdx);
            for (FieldValue fv : flatDoc.fields()) {
                DocumentBatchEncoder.ColumnBuilder col = columns.computeIfAbsent(
                    fv.fieldPath,
                    k -> new DocumentBatchEncoder.ColumnBuilder(docCount)
                );
                fv.applyTo(col, docIdx);
            }

            // Ensure all columns known so far have an entry for this doc
            for (DocumentBatchEncoder.ColumnBuilder col : columns.values()) {
                col.ensureSize(docIdx + 1);
            }
        }

        // Phase 2: Build the IndexRequest list for serialization metadata
        List<IndexRequest> requests = new ArrayList<>(docCount);
        for (FlattenedDoc flatDoc : flatDocs) {
            requests.add(flatDoc.request());
        }

        // Phase 3: Serialize using the shared serialize method
        DocumentBatch batch = DocumentBatchEncoder.serialize(requests, columns, docCount);

        // Phase 4: Extract tsids if present
        if (requests.getFirst().tsid() != null) {
            BytesRef[] tsids = new BytesRef[docCount];
            for (int i = 0; i < docCount; i++) {
                tsids[i] = requests.get(i).tsid();
            }
            batch.setTsids(tsids);
        }

        return batch;
    }
}
