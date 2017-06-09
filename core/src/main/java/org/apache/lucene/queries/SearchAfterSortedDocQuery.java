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

package org.apache.lucene.queries;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.EarlyTerminatingSortingCollector;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A {@link Query} that only matches documents that are greater than the provided {@link FieldDoc}.
 * This works only if the index is sorted according to the given search {@link Sort}.
 */
public class SearchAfterSortedDocQuery extends Query {
    private final Sort sort;
    private final FieldDoc after;
    private final FieldComparator<?>[] fieldComparators;
    private final int[] reverseMuls;

    public SearchAfterSortedDocQuery(Sort sort, FieldDoc after) {
        if (sort.getSort().length != after.fields.length) {
            throw new IllegalArgumentException("after doc  has " + after.fields.length + " value(s) but sort has "
                    + sort.getSort().length + ".");
        }
        this.sort = sort;
        this.after = after;
        int numFields = sort.getSort().length;
        this.fieldComparators = new FieldComparator[numFields];
        this.reverseMuls = new int[numFields];
        for (int i = 0; i < numFields; i++) {
            SortField sortField = sort.getSort()[i];
            FieldComparator<?> fieldComparator = sortField.getComparator(1, i);
            @SuppressWarnings("unchecked")
            FieldComparator<Object> comparator = (FieldComparator<Object>) fieldComparator;
            comparator.setTopValue(after.fields[i]);
            fieldComparators[i] = fieldComparator;
            reverseMuls[i] = sortField.getReverse() ? -1 : 1;
        }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
        return new ConstantScoreWeight(this, 1.0f) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                Sort segmentSort = context.reader().getMetaData().getSort();
                if (EarlyTerminatingSortingCollector.canEarlyTerminate(sort, segmentSort) == false) {
                    throw new IOException("search sort :[" + sort.getSort() + "] does not match the index sort:[" + segmentSort + "]");
                }
                final int afterDoc = after.doc - context.docBase;
                TopComparator comparator= getTopComparator(fieldComparators, reverseMuls, context, afterDoc);
                final int maxDoc = context.reader().maxDoc();
                final int firstDoc = searchAfterDoc(comparator, 0, context.reader().maxDoc());
                if (firstDoc >= maxDoc) {
                    return null;
                }
                final DocIdSetIterator disi = new MinDocQuery.MinDocIterator(firstDoc, maxDoc);
                return new ConstantScoreScorer(this, score(), disi);
            }
        };
    }

    @Override
    public String toString(String field) {
        return "SearchAfterSortedDocQuery(sort=" + sort  + ", afterDoc=" + after.toString() + ")";
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) &&
            equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(SearchAfterSortedDocQuery other) {
        return sort.equals(other.sort) &&
            after.doc == other.after.doc &&
            Double.compare(after.score, other.after.score) == 0 &&
            Arrays.equals(after.fields, other.after.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), sort, after.doc, after.score, Arrays.hashCode(after.fields));
    }

    interface TopComparator {
        boolean lessThanTop(int doc) throws IOException;
    }

    static TopComparator getTopComparator(FieldComparator<?>[] fieldComparators,
                                          int[] reverseMuls,
                                          LeafReaderContext leafReaderContext,
                                          int topDoc) {
        return doc -> {
            // DVs use forward iterators so we recreate the iterator for each sort field
            // every time we need to compare a document with the <code>after<code> doc.
            // We could reuse the iterators when the comparison goes forward but
            // this should only be called a few time per segment (binary search).
            for (int i = 0; i < fieldComparators.length; i++) {
                LeafFieldComparator comparator =  fieldComparators[i].getLeafComparator(leafReaderContext);
                int value = reverseMuls[i] * comparator.compareTop(doc);
                if (value != 0) {
                    return value < 0;
                }
            }

            if (topDoc <= doc) {
                return false;
            }
            return true;
        };
    }

    /**
     * Returns the first doc id greater than the provided <code>after</code> doc.
     */
    static int searchAfterDoc(TopComparator comparator, int from, int to) throws IOException {
        int low = from;
        int high = to - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (comparator.lessThanTop(mid)) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return low;
    }

}
