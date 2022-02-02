/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.util.Objects;

public abstract class ScoreFunction {

    private final CombineFunction scoreCombiner;

    protected ScoreFunction(CombineFunction scoreCombiner) {
        this.scoreCombiner = scoreCombiner;
    }

    public CombineFunction getDefaultScoreCombiner() {
        return scoreCombiner;
    }

    public abstract LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException;

    /**
     * Indicates if document scores are needed by this function.
     *
     * @return {@code true} if scores are needed.
     */
    public abstract boolean needsScores();

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ScoreFunction other = (ScoreFunction) obj;
        return Objects.equals(scoreCombiner, other.scoreCombiner) && doEquals(other);
    }

    public float getWeight() {
        return 1.0f;
    }

    /**
     * Indicates whether some other {@link ScoreFunction} object of the same type is "equal to" this one.
     */
    protected abstract boolean doEquals(ScoreFunction other);

    @Override
    public final int hashCode() {
        /*
         * Override hashCode here and forward to an abstract method to force extensions of this class to override hashCode in the same
         * way that we force them to override equals. This also prevents false positives in CheckStyle's EqualsHashCode check.
         */
        return Objects.hash(scoreCombiner, doHashCode());
    }

    protected abstract int doHashCode();

    protected ScoreFunction rewrite(IndexReader reader) throws IOException {
        return this;
    }
}
