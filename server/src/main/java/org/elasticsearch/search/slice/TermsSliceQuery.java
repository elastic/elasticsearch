/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.slice;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.StringHelper;

import java.io.IOException;

/**
 * A {@link SliceQuery} that uses the terms dictionary of a field to do the slicing.
 *
 * <b>NOTE</b>: The cost of this filter is O(N*M) where N is the number of unique terms in the dictionary
 * and M is the average number of documents per term.
 * For each segment this filter enumerates the terms dictionary, computes the hash code for each term and fills
 * a bit set with the documents of all terms whose hash code matches the predicate.
 * <b>NOTE</b>: Documents with no value for that field are ignored.
 */
public final class TermsSliceQuery extends SliceQuery {
    // Fixed seed for computing term hashCode
    public static final int SEED = 7919;

    public TermsSliceQuery(String field, int id, int max) {
        super(field, id, max);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                final DocIdSet disi = build(context.reader());
                final DocIdSetIterator leafIt = disi.iterator();
                return new ConstantScoreScorer(this, score(), scoreMode, leafIt);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
        };
    }

    /**
     * Returns a DocIdSet per segments containing the matching docs for the specified slice.
     */
    private DocIdSet build(LeafReader reader) throws IOException {
        final DocIdSetBuilder builder = new DocIdSetBuilder(reader.maxDoc());
        final Terms terms = reader.terms(getField());
        if (terms == null) {
            return DocIdSet.EMPTY;
        }
        final TermsEnum te = terms.iterator();
        PostingsEnum docsEnum = null;
        for (BytesRef term = te.next(); term != null; term = te.next()) {
            // use a fixed seed instead of term.hashCode() otherwise this query may return inconsistent results when
            // running on another replica (StringHelper sets its default seed at startup with current time)
            int hashCode = StringHelper.murmurhash3_x86_32(term, SEED);
            if (contains(hashCode)) {
                docsEnum = te.postings(docsEnum, PostingsEnum.NONE);
                builder.add(docsEnum);
            }
        }
        return builder.build();
    }
}
