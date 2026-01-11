/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import java.io.IOException;
import java.util.Arrays;


import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;

/**
 * Simple query that exposes per-segment {@link AcceptDocs} as a query.
 * This lets us benchmark / filter using the AcceptDocs interface
 * instead of materializing a BitSet.
 */
public class AcceptDocsQuery extends Query {

    private final AcceptDocs[] segmentAcceptDocs;

    public AcceptDocsQuery(AcceptDocs[] segmentAcceptDocs) {
        this.segmentAcceptDocs = segmentAcceptDocs;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                AcceptDocs acceptDocs = segmentAcceptDocs[context.ord];

                // Build iterator once per leaf; allowed to throw IOException here
                DocIdSetIterator iterator = acceptDocs.iterator();
                final long cost = acceptDocs.cost();

                final Scorer scorer = new ConstantScoreScorer(score(), scoreMode, iterator);

                return new ScorerSupplier() {
                    @Override
                    public Scorer get(long leadCost) {
                        return scorer;
                    }

                    @Override
                    public long cost() {
                        return cost;
                    }
                };
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        // no children
        visitor.visitLeaf(this);
    }

    @Override
    public String toString(String field) {
        return "AcceptDocsQuery";
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        AcceptDocsQuery other = (AcceptDocsQuery) obj;
        return Arrays.equals(this.segmentAcceptDocs, other.segmentAcceptDocs);
    }

    @Override
    public int hashCode() {
        int result = classHash();
        result = 31 * result + Arrays.hashCode(segmentAcceptDocs);
        return result;
    }

}
