/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.RamUsageEstimates;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.xcontent.Text;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Map;

/**
 * Estimates the retained heap of a {@link SearchHit}. Not every field is counted, and the value can grow as the hit
 * materializes more of itself, so a caller charging a breaker must release the amount it charged, never a fresh estimate.
 */
public final class SearchHitRamUsageEstimator {

    private static final long SEARCH_HIT_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(SearchHit.class);
    private static final long SEARCH_HITS_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(SearchHits.class);
    private static final long HIGHLIGHT_FIELD_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(HighlightField.class);
    private static final long TEXT_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Text.class);
    private static final long EXPLANATION_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Explanation.class);
    private static final long FLOAT_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Float.class);
    private static final long HASH_MAP_NODE_SIZE = RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + Integer.BYTES + 3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF
    );
    // LinkedHashMap.Entry adds a before/after pair to HashMap.Node
    private static final long LINKED_HASH_MAP_NODE_SIZE = RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + Integer.BYTES + 5L * RamUsageEstimator.NUM_BYTES_OBJECT_REF
    );
    /**
     * Wrapper {@link Collections#unmodifiableList} puts around an {@link Explanation}'s details. This and the
     * {@link ArrayList} term below track how Lucene currently builds that list; revisit on an upgrade that changes it,
     * since the tests assert relative growth rather than exact bytes and would not catch the drift.
     */
    private static final long UNMODIFIABLE_LIST_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOf(
        Collections.unmodifiableList(new ArrayList<>())
    );

    /**
     * Bounds the per-hit walk of an explanation tree, which runs on the fetch thread. Past this the walk stops and the
     * result becomes a floor rather than an upper bound. Nothing keeps a tree under it:
     * {@code indices.query.bool.max_clause_count} is capped only by {@link Integer#MAX_VALUE}, and nested bool queries
     * multiply clauses up to {@code indices.query.bool.max_nested_depth}. Sub-explanations shared across branches reach
     * it far sooner, being visited once per path while retained heap does not grow.
     */
    static final int MAX_EXPLANATION_NODES = 100_000;

    /**
     * Charged for whatever {@link #MAX_EXPLANATION_NODES} left unvisited. A placeholder, not a measurement: it
     * over-estimates a shared tree, whose nodes are already counted once per path, and under-estimates a huge distinct one.
     */
    static final long EXPLANATION_NODE_CAP_PENALTY_BYTES = 1L << 20;

    static final long RAM_BYTES_FLOOR = 512L;

    private SearchHitRamUsageEstimator() {}

    /**
     * Returns the estimated retained heap of everything the fetch sub-phases attach to {@code hit}: the
     * {@link SearchHit#getDocumentFields() document} and {@link SearchHit#getMetadataFields() metadata} fields, the
     * {@link SearchHit#getHighlightFields() highlight} fragments, the {@link SearchHit#getExplanation() explanation}
     * tree, and the {@link SearchHit#getMatchedQueriesAndScores() matched queries}.
     * <p>
     * Both exclusions avoid double-counting: {@code FetchPhase} charges the source against its own buffer, and inner
     * hits are charged by the nested fetch and transferred to the parent context by {@code InnerHitsPhase}.
     */
    public static long estimateSubPhaseOutput(SearchHit hit) {
        long size = estimateFields(hit.getDocumentFields());
        size += estimateFields(hit.getMetadataFields());
        size += estimateHighlightFields(hit.getHighlightFields());
        size += estimateExplanation(hit.getExplanation());
        size += estimateMatchedQueries(hit.getMatchedQueriesAndScores());
        return size;
    }

    public static long estimate(SearchHit hit) {
        long size = SEARCH_HIT_SHALLOW_SIZE + RAM_BYTES_FLOOR + hit.rawSourceLength();
        size += estimateSubPhaseOutput(hit);
        Map<String, SearchHits> innerHits = hit.getInnerHits();
        if (innerHits != null) {
            size += RamUsageEstimates.HASH_MAP_SHALLOW_SIZE + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) innerHits.size()
                * (HASH_MAP_NODE_SIZE + RamUsageEstimator.NUM_BYTES_OBJECT_REF);
            for (Map.Entry<String, SearchHits> entry : innerHits.entrySet()) {
                size += RamUsageEstimator.sizeOf(entry.getKey());
                SearchHit[] innerHitsArray = entry.getValue().getHits();
                size += SEARCH_HITS_SHALLOW_SIZE + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) innerHitsArray.length
                    * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
                for (SearchHit innerHit : innerHitsArray) {
                    size += estimate(innerHit);
                }
            }
        }
        return size;
    }

    private static long estimateFields(Map<String, DocumentField> fields) {
        if (fields == null || fields.isEmpty()) {
            return 0L;
        }
        long size = RamUsageEstimates.HASH_MAP_SHALLOW_SIZE + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) fields.size()
            * (HASH_MAP_NODE_SIZE + RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        for (DocumentField field : fields.values()) {
            size += field.ramBytesUsedEstimate();
        }
        return size;
    }

    private static long estimateHighlightFields(Map<String, HighlightField> fields) {
        if (fields.isEmpty()) {
            return 0L;
        }
        long size = RamUsageEstimates.HASH_MAP_SHALLOW_SIZE + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) fields.size()
            * (HASH_MAP_NODE_SIZE + RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        for (HighlightField field : fields.values()) {
            size += HIGHLIGHT_FIELD_SHALLOW_SIZE + RamUsageEstimator.sizeOf(field.name());
            Text[] fragments = field.fragments();
            if (fragments != null) {
                size += RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) fragments.length * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
                for (Text fragment : fragments) {
                    size += estimateFragment(fragment);
                }
            }
        }
        return size;
    }

    /**
     * Only counts the views a fragment has already materialized, since asking for the other one would build it.
     */
    private static long estimateFragment(Text fragment) {
        long size = TEXT_SHALLOW_SIZE;
        if (fragment.hasBytes()) {
            size += RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + fragment.bytes().length();
        }
        if (fragment.hasString()) {
            size += RamUsageEstimator.sizeOf(fragment.string());
        }
        return size;
    }

    /**
     * Walks the tree iteratively: it mirrors the query, so its depth grows with clause nesting and recursion would risk
     * a stack overflow on the fetch thread.
     */
    private static long estimateExplanation(Explanation explanation) {
        if (explanation == null) {
            return 0L;
        }
        long size = 0L;
        int visited = 0;
        Deque<Explanation> pending = new ArrayDeque<>();
        pending.push(explanation);
        while (pending.isEmpty() == false) {
            if (visited == MAX_EXPLANATION_NODES) {
                return size + EXPLANATION_NODE_CAP_PENALTY_BYTES;
            }
            Explanation node = pending.pop();
            visited++;
            size += EXPLANATION_SHALLOW_SIZE;
            size += RamUsageEstimator.sizeOf(node.getDescription());
            // Always a boxed float in practice; shallowSizeOf falls back to uncached reflection per node.
            Number value = node.getValue();
            size += value instanceof Float ? FLOAT_SHALLOW_SIZE : RamUsageEstimator.shallowSizeOf(value);
            // An unmodifiable view over an exactly sized ArrayList, both allocated even when empty.
            size += UNMODIFIABLE_LIST_SHALLOW_SIZE + RamUsageEstimates.ARRAY_LIST_SHALLOW_SIZE;
            // getDetails() copies into a fresh array per call, so ask once and size the retained list instead.
            Explanation[] details = node.getDetails();
            if (details.length > 0) {
                size += RamUsageEstimator.alignObjectSize(
                    RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) details.length * RamUsageEstimator.NUM_BYTES_OBJECT_REF
                );
                for (Explanation detail : details) {
                    pending.push(detail);
                }
            }
        }
        return size;
    }

    private static long estimateMatchedQueries(Map<String, Float> matchedQueries) {
        if (matchedQueries.isEmpty()) {
            return 0L;
        }
        long size = RamUsageEstimates.LINKED_HASH_MAP_SHALLOW_SIZE + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) matchedQueries.size()
            * (LINKED_HASH_MAP_NODE_SIZE + RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        for (String name : matchedQueries.keySet()) {
            size += RamUsageEstimator.sizeOf(name) + FLOAT_SHALLOW_SIZE;
        }
        return size;
    }
}
