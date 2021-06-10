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
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

import java.io.IOException;

/**
 * A {@link SliceQuery} that partitions documents based on their Lucene ID.
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
        super(id, max);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public Scorer scorer(LeafReaderContext context) {
                DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
                TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {
                    @Override
                    public boolean matches() {
                        int docId = context.docBase + approximation.docID();
                        return contains(docId);
                    }
                    @Override
                    public float matchCost() {
                        return 3;
                    }
                };
                return new ConstantScoreScorer(this, score(), scoreMode, twoPhase);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
        };
    }

    @Override
    protected boolean doEquals(SliceQuery o) {
        return true;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }

    @Override
    public String toString(String f) {
        return getClass().getSimpleName() + "[id=" + getId() + ", max=" + getMax() + "]";
    }
}
