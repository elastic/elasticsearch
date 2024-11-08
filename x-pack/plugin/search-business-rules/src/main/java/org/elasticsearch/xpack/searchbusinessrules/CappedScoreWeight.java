/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

import java.io.IOException;

/**
 * A Weight that caps scores of the wrapped query to a maximum value
 */
public abstract class CappedScoreWeight extends Weight {

    private final float maxScore;
    private final Weight innerWeight;

    protected CappedScoreWeight(Query query, Weight innerWeight, float maxScore) {
        super(query);
        this.maxScore = maxScore;
        this.innerWeight = innerWeight;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return innerWeight.isCacheable(ctx);
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        ScorerSupplier innerScorerSupplier = innerWeight.scorerSupplier(context);
        if (innerScorerSupplier == null) {
            return null;
        }
        return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
                return new CappedScorer(innerScorerSupplier.get(leadCost), maxScore);
            }

            @Override
            public long cost() {
                return innerScorerSupplier.cost();
            }
        };
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {

        final Scorer s = scorer(context);
        final boolean exists;
        if (s == null) {
            exists = false;
        } else {
            final TwoPhaseIterator twoPhase = s.twoPhaseIterator();
            if (twoPhase == null) {
                exists = s.iterator().advance(doc) == doc;
            } else {
                exists = twoPhase.approximation().advance(doc) == doc && twoPhase.matches();
            }
        }

        Explanation sub = innerWeight.explain(context, doc);
        if (sub.isMatch() && sub.getValue().floatValue() > maxScore) {
            return Explanation.match(
                maxScore,
                "Capped score of " + innerWeight.getQuery() + ", max of",
                sub,
                Explanation.match(maxScore, "maximum score")
            );
        } else {
            return sub;
        }
    }

}
