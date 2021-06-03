/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.apache.lucene.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;

import java.io.IOException;
import java.util.Set;

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
    public void extractTerms(Set<Term> terms) {
        innerWeight.extractTerms(terms);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return innerWeight.isCacheable(ctx);
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
        return new CappedScorer(this, innerWeight.scorer(context), maxScore);
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
          return Explanation.match(maxScore, "Capped score of " + innerWeight.getQuery() + ", max of",
              sub,
              Explanation.match(maxScore, "maximum score"));
        } else {
          return sub;
        }
    }

}
