/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.slice;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
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
 * A {@link SliceQuery} that uses the numeric doc values of a field to do the slicing.
 *
 * <b>NOTE</b>: With deterministic field values this query can be used across different readers safely.
 * If updates are accepted on the field you must ensure that the same reader is used for all `slice` queries.
 */
public final class DocValuesSliceQuery extends SliceQuery {
    public DocValuesSliceQuery(String field, int id, int max) {
        super(field, id, max);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                final SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), getField());
                final DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());
                final TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {

                    @Override
                    public boolean matches() throws IOException {
                        if (values.advanceExact(approximation.docID())) {
                            for (int i = 0; i < values.docValueCount(); i++) {
                                if (contains(BitMixer.mix(values.nextValue()))) {
                                    return true;
                                }
                            }
                            return false;
                        } else {
                            return contains(0);
                        }
                    }

                    @Override
                    public float matchCost() {
                        // BitMixer.mix seems to be about 10 ops
                        return 10;
                    }
                };
                return new ConstantScoreScorer(this, score(), scoreMode, twoPhase);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, getField());
            }

        };
    }
}
