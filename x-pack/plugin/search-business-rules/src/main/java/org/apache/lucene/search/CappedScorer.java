/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.apache.lucene.search;

import java.io.IOException;

public class CappedScorer extends FilterScorer {
    private final float maxScore;

    public CappedScorer(Weight weight, Scorer delegate, float maxScore) {
        super(delegate, weight);
        this.maxScore = maxScore;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        return Math.min(maxScore, in.getMaxScore(upTo));
    }

    @Override
    public int advanceShallow(int target) throws IOException {
        return in.advanceShallow(target);
    }

    @Override
    public float score() throws IOException {
        return Math.min(maxScore, in.score());
    }

}