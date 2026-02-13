/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.Arrays;
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
            throw new IllegalArgumentException(
                "after doc  has " + after.fields.length + " value(s) but sort has " + sort.getSort().length + "."
            );
        }
        this.sort = Objects.requireNonNull(sort);
        this.after = after;
        int numFields = sort.getSort().length;
        this.fieldComparators = new FieldComparator<?>[numFields];
        this.reverseMuls = new int[numFields];
        for (int i = 0; i < numFields; i++) {
            SortField sortField = sort.getSort()[i];
            FieldComparator<?> fieldComparator = sortField.getComparator(1, Pruning.NONE);
            @SuppressWarnings("unchecked")
            FieldComparator<Object> comparator = (FieldComparator<Object>) fieldComparator;
            comparator.setTopValue(after.fields[i]);
            fieldComparators[i] = fieldComparator;
            reverseMuls[i] = sortField.getReverse() ? -1 : 1;
        }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, 1.0f) {
            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                Sort segmentSort = context.reader().getMetaData().sort();
                if (segmentSort == null || Lucene.canEarlyTerminate(sort, segmentSort) == false) {
                    throw new IOException("search sort :[" + sort + "] does not match the index sort:[" + segmentSort + "]");
                }
                final int afterDoc = after.doc - context.docBase;
                TopComparator comparator = getTopComparator(fieldComparators, reverseMuls, context, afterDoc);
                final int maxDoc = context.reader().maxDoc();
                final int firstDoc = searchAfterDoc(comparator, 0, context.reader().maxDoc());
                if (firstDoc >= maxDoc) {
                    return null;
                }
                final DocIdSetIterator disi = new MinDocQuery.MinDocIterator(firstDoc, maxDoc);
                Scorer scorer = new ConstantScoreScorer(score(), scoreMode, disi);
                return new DefaultScorerSupplier(scorer);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // If the sort order includes _doc, then the matches in a segment
                // may depend on other segments, which makes this query a bad
                // candidate for caching
                return false;
            }
        };
    }

    @Override
    public String toString(String field) {
        return "SearchAfterSortedDocQuery(sort=" + sort + ", afterDoc=" + after.toString() + ")";
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(SearchAfterSortedDocQuery other) {
        return sort.equals(other.sort)
            && after.doc == other.after.doc
            && Double.compare(after.score, other.after.score) == 0
            && Arrays.equals(after.fields, other.after.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), sort, after.doc, after.score, Arrays.hashCode(after.fields));
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    interface TopComparator {
        boolean lessThanTop(int doc) throws IOException;
    }

    static TopComparator getTopComparator(
        FieldComparator<?>[] fieldComparators,
        int[] reverseMuls,
        LeafReaderContext leafReaderContext,
        int topDoc
    ) {
        return doc -> {
            // DVs use forward iterators so we recreate the iterator for each sort field
            // every time we need to compare a document with the <code>after<code> doc.
            // We could reuse the iterators when the comparison goes forward but
            // this should only be called a few time per segment (binary search).
            for (int i = 0; i < fieldComparators.length; i++) {
                LeafFieldComparator comparator = fieldComparators[i].getLeafComparator(leafReaderContext);
                int value = reverseMuls[i] * comparator.compareTop(doc);
                if (value != 0) {
                    return value < 0;
                }
            }

            if (doc <= topDoc) {
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
