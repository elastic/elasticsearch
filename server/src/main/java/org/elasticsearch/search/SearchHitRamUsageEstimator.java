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

import java.util.HashMap;
import java.util.Map;

/**
 * Conservative upper-bound estimator for the retained heap of a {@link SearchHit}
 */
public final class SearchHitRamUsageEstimator {

    private static final long SEARCH_HIT_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(SearchHit.class);
    private static final long HASH_MAP_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(HashMap.class);
    private static final long SEARCH_HITS_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(SearchHits.class);
    private static final long HASH_MAP_NODE_SIZE = RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + Integer.BYTES + 3L * RamUsageEstimator.NUM_BYTES_OBJECT_REF
    );

    static final long RAM_BYTES_FLOOR = 512L;

    private SearchHitRamUsageEstimator() {}

    public static long estimate(SearchHit hit) {
        long size = SEARCH_HIT_SHALLOW_SIZE + RAM_BYTES_FLOOR + hit.rawSourceLength();
        size += estimateFields(hit.getDocumentFields());
        size += estimateFields(hit.getMetadataFields());
        Map<String, SearchHits> innerHits = hit.getInnerHits();
        if (innerHits != null) {
            size += HASH_MAP_SHALLOW_SIZE + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) innerHits.size() * (HASH_MAP_NODE_SIZE
                + RamUsageEstimator.NUM_BYTES_OBJECT_REF);
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
        long size = HASH_MAP_SHALLOW_SIZE + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) fields.size() * (HASH_MAP_NODE_SIZE
            + RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        for (DocumentField field : fields.values()) {
            size += field.ramBytesUsedEstimate();
        }
        return size;
    }
}
