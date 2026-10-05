/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.lucene.RamUsageEstimates;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.xcontent.Text;

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
    private static final long HASH_MAP_NODE_SIZE = RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + Integer.BYTES + 3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF
    );

    static final long RAM_BYTES_FLOOR = 512L;

    private SearchHitRamUsageEstimator() {}

    public static long estimate(SearchHit hit) {
        long size = SEARCH_HIT_SHALLOW_SIZE + RAM_BYTES_FLOOR + hit.rawSourceLength();
        size += estimateFields(hit.getDocumentFields());
        size += estimateFields(hit.getMetadataFields());
        size += estimateHighlightFields(hit.getHighlightFields());
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
}
