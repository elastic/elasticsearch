/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;

class ScoreOne extends ScoreFunction {

    protected ScoreOne() {
        super(CombineFunction.MULTIPLY);
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) {
        return new LeafScoreFunction() {
            @Override
            public double score(int docId, float subQueryScore) {
                return 1.0;
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) {
                return Explanation.match(1.0f, "constant score 1.0 - no function provided");
            }
        };
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    protected boolean doEquals(ScoreFunction other) {
        return true;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }
}
