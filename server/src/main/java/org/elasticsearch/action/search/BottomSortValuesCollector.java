/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchSortValuesAndFormats;

/**
 * Utility class to keep track of the bottom doc's sort values in a distributed search.
 */
class BottomSortValuesCollector {
    private final int topNSize;
    private final SortField[] sortFields;
    private final FieldComparator[] comparators;
    private final int[] reverseMuls;

    private volatile long totalHits;
    private volatile SearchSortValuesAndFormats bottomSortValues;

    BottomSortValuesCollector(int topNSize, SortField[] sortFields) {
        this.topNSize = topNSize;
        this.comparators = new FieldComparator[sortFields.length];
        this.reverseMuls = new int[sortFields.length];
        this.sortFields = sortFields;
        for (int i = 0; i < sortFields.length; i++) {
            comparators[i] = sortFields[i].getComparator(1, i);
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
        if (bottomSortValues == null
                || compareValues(shardBottomDoc.fields, bottomSortValues.getRawSortValues()) < 0) {
            bottomSortValues = new SearchSortValuesAndFormats(shardBottomDoc.fields, sortValuesFormat);
        }
    }

    /**
     * @return <code>false</code> if the provided {@link SortField} array differs
     * from the initial {@link BottomSortValuesCollector#sortFields}.
     */
    private boolean validateShardSortFields(SortField[] shardSortFields) {
        for (int i = 0; i  < shardSortFields.length; i++) {
            if (shardSortFields[i].equals(sortFields[i]) == false) {
                // ignore shards response that would make the sort incompatible
                // (e.g.: mixing keyword/numeric or long/double).
                // TODO: we should fail the entire request because the topdocs
                //  merge will likely fail later but this is not possible with
                //  the current async logic that only allows shard failures here.
                return false;
            }
        }
        return true;
    }

    private FieldDoc extractBottom(TopFieldDocs topDocs) {
        return topNSize > 0 && topDocs.scoreDocs.length == topNSize ?
            (FieldDoc) topDocs.scoreDocs[topNSize-1] : null;
    }

    private int compareValues(Object[] v1, Object[] v2) {
        for (int i = 0; i < v1.length; i++) {
            int cmp = reverseMuls[i] * comparators[i].compareValues(v1[i], v2[i]);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }
}
