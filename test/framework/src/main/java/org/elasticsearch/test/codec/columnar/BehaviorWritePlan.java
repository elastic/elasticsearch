/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.codec.columnar;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

/**
 * Controls how a corpus is written into an index so the segment layout can be varied: a single batch, several
 * refresh-separated batches (final multiple segments), or several batches followed by a force merge. The same plan
 * is applied to both indices in a duel pair so any behavioral difference is attributable to the doc-values
 * codec rather than to a different segment geometry. Segment counts are intentionally not asserted; only the
 * number of batches and the optional force-merge target are controlled.
 */
public final class BehaviorWritePlan {

    private static final String DOC_ID_FIELD = "doc_id";

    private final int batches;
    private final int finalMaxSegments;

    private BehaviorWritePlan(int batches, int finalMaxSegments) {
        this.batches = batches;
        this.finalMaxSegments = finalMaxSegments;
    }

    /**
     * @return a plan that writes the whole corpus in one refreshed batch.
     */
    public static BehaviorWritePlan singleBatch() {
        return new BehaviorWritePlan(1, 0);
    }

    /**
     * @param batches the number of refresh-separated batches
     * @return a plan that writes the corpus in {@code batches} batches, building multiple segments.
     */
    public static BehaviorWritePlan multiSegment(int batches) {
        return new BehaviorWritePlan(Math.max(1, batches), 0);
    }

    /**
     * @param batches          the number of refresh-separated batches
     * @param finalMaxSegments the force-merge target applied after indexing
     * @return a plan that writes the corpus in batches then force merges down to {@code finalMaxSegments}.
     */
    public static BehaviorWritePlan multiSegmentThenForceMerge(int batches, int finalMaxSegments) {
        return new BehaviorWritePlan(Math.max(1, batches), Math.max(1, finalMaxSegments));
    }

    /**
     * @return a randomly chosen plan, seeded through the Elasticsearch test seed so it reproduces.
     */
    public static BehaviorWritePlan random() {
        return switch (randomInt(2)) {
            case 0 -> singleBatch();
            case 1 -> multiSegment(randomIntBetween(2, 4));
            case 2 -> multiSegmentThenForceMerge(randomIntBetween(2, 4), 1);
            default -> throw new AssertionError("unreachable");
        };
    }

    /**
     * Writes {@code docs} into {@code indexName} following this plan. Deterministic for a given batch count so
     * both indices in a pair receive the same batch boundaries.
     *
     * @param client       the client to index through
     * @param indexName    the target index
     * @param docs         the corpus
     * @param keywordField the keyword field name
     */
    public void apply(final Client client, final String indexName, final List<KeywordDoc> docs, final String keywordField) {
        for (final List<KeywordDoc> chunk : split(docs)) {
            bulkIndex(client, indexName, chunk, keywordField);
            client.admin().indices().prepareRefresh(indexName).get();
        }
        if (finalMaxSegments > 0) {
            client.admin().indices().prepareForceMerge(indexName).setMaxNumSegments(finalMaxSegments).get();
            client.admin().indices().prepareRefresh(indexName).get();
        }
    }

    private List<List<KeywordDoc>> split(final List<KeywordDoc> docs) {
        final int effectiveBatches = Math.min(batches, Math.max(1, docs.size()));
        final List<List<KeywordDoc>> chunks = new ArrayList<>(effectiveBatches);
        final int chunkSize = (docs.size() + effectiveBatches - 1) / effectiveBatches;
        for (int start = 0; start < docs.size(); start += chunkSize) {
            chunks.add(docs.subList(start, Math.min(start + chunkSize, docs.size())));
        }
        return chunks;
    }

    private static void bulkIndex(final Client client, final String indexName, final List<KeywordDoc> docs, final String keywordField) {
        final BulkRequestBuilder bulk = client.prepareBulk();
        for (final KeywordDoc doc : docs) {
            final Map<String, Object> source = new LinkedHashMap<>();
            source.put(DOC_ID_FIELD, doc.docId());
            if (doc.values() != null) {
                source.put(keywordField, doc.values());
            }
            bulk.add(client.prepareIndex(indexName).setId(doc.id()).setSource(source));
        }
        final BulkResponse response = bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE).get();
        if (response.hasFailures()) {
            throw new AssertionError("bulk indexing into [" + indexName + "] failed: " + response.buildFailureMessage());
        }
    }

    @Override
    public String toString() {
        return "batches=" + batches + " finalMaxSegments=" + finalMaxSegments;
    }
}
