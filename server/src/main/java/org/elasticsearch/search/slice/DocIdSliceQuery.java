/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.slice;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.search.sort.FieldSortBuilder;

import java.io.IOException;

/**
 * A {@link SliceQuery} that partitions documents based on their Lucene ID. To take
 * advantage of locality, each slice holds a contiguous range of document IDs.
 *
 * NOTE: Because the query relies on Lucene document IDs, it is not stable across
 * readers. It's intended for scenarios where the reader doesn't change, like in
 * a point-in-time search.
 */
public final class DocIdSliceQuery extends SliceQuery {

    /**
     * @param id    The id of the slice
     * @param max   The maximum number of slices
     */
    public DocIdSliceQuery(int id, int max) {
        super(FieldSortBuilder.DOC_FIELD_NAME, id, max);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        int maxDoc = searcher.getTopReaderContext().reader().maxDoc();

        int remainder = maxDoc % getMax();
        int quotient = maxDoc / getMax();

        int sliceStart;
        int sliceSize;
        if (getId() < remainder) {
            sliceStart = (quotient + 1) * getId();
            sliceSize = quotient + 1;
        } else {
            sliceStart = remainder * (quotient + 1) + (getId() - remainder) * quotient;
            sliceSize = quotient;
        }

        return new ConstantScoreWeight(this, boost) {
            @Override
            public Scorer scorer(LeafReaderContext context) {
                DocIdSetIterator iterator = createIterator(context, sliceStart, sliceStart + sliceSize);
                return new ConstantScoreScorer(this, boost, scoreMode, iterator);
            }

            private static DocIdSetIterator createIterator(LeafReaderContext context, int sliceStart, int sliceEnd) {
                int leafStart = context.docBase;
                int leafEnd = context.docBase + context.reader().maxDoc();

                // There is no overlap with this segment, so return empty iterator
                if (leafEnd <= sliceStart || leafStart >= sliceEnd) {
                    return DocIdSetIterator.empty();
                }

                int start = Math.max(leafStart, sliceStart) - context.docBase;
                int end = Math.min(leafEnd, sliceEnd) - context.docBase;
                return DocIdSetIterator.range(start, end);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }
}
