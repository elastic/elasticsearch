/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchSortValuesAndFormats;

import static org.elasticsearch.core.Types.forciblyCast;

/**
 * Utility class to keep track of the bottom doc's sort values in a distributed search.
 */
class BottomSortValuesCollector {
    private final int topNSize;
    private final SortField[] sortFields;
    private final FieldComparator<?>[] comparators;
    private final int[] reverseMuls;

    private volatile long totalHits;
    private volatile SearchSortValuesAndFormats bottomSortValues;

    BottomSortValuesCollector(int topNSize, SortField[] sortFields) {
        this.topNSize = topNSize;
        this.comparators = new FieldComparator<?>[sortFields.length];
        this.reverseMuls = new int[sortFields.length];
        this.sortFields = sortFields;
        for (int i = 0; i < sortFields.length; i++) {
            comparators[i] = sortFields[i].getComparator(1, false);
            reverseMuls[i] = sortFields[i].getReverse() ? -1 : 1;
        }
    }

    long getTotalHits() {
        return totalHits;
    }

    /**
     * @return The best bottom sort values consumed so far.
     */
    SearchSortValuesAndFormats getBottomSortValues() {
        return bottomSortValues;
    }

    synchronized void consumeTopDocs(TopFieldDocs topDocs, DocValueFormat[] sortValuesFormat) {
        totalHits += topDocs.totalHits.value;
        if (validateShardSortFields(topDocs.fields) == false) {
            return;
        }

        FieldDoc shardBottomDoc = extractBottom(topDocs);
        if (shardBottomDoc == null) {
            return;
        }
        if (bottomSortValues == null || compareValues(shardBottomDoc.fields, bottomSortValues.getRawSortValues()) < 0) {
            bottomSortValues = new SearchSortValuesAndFormats(shardBottomDoc.fields, sortValuesFormat);
        }
    }

    /**
     * @return <code>false</code> if the provided {@link SortField} array differs
     * from the initial {@link BottomSortValuesCollector#sortFields}.
     */
    private boolean validateShardSortFields(SortField[] shardSortFields) {
        for (int i = 0; i < shardSortFields.length; i++) {
            if (shardSortFields[i].equals(sortFields[i]) == false) {
                // ignore shards response that would make the sort incompatible
                // (e.g.: mixing keyword/numeric or long/double).
                // TODO: we should fail the entire request because the topdocs
                // merge will likely fail later but this is not possible with
                // the current async logic that only allows shard failures here.
                return false;
            }
        }
        return true;
    }

    private FieldDoc extractBottom(TopFieldDocs topDocs) {
        return topNSize > 0 && topDocs.scoreDocs.length == topNSize ? (FieldDoc) topDocs.scoreDocs[topNSize - 1] : null;
    }

    private int compareValues(Object[] v1, Object[] v2) {
        for (int i = 0; i < v1.length; i++) {
            int cmp = reverseMuls[i] * comparators[i].compareValues(forciblyCast(v1[i]), forciblyCast(v2[i]));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }
}
