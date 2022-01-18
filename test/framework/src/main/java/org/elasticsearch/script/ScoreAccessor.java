/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.search.Scorable;

import java.io.IOException;

/**
 * A float encapsulation that dynamically accesses the score of a document.
 *
 * The provided {@link Scorable} is used to retrieve the score
 * for the current document.
 */
public final class ScoreAccessor extends Number {

    Scorable scorer;

    public ScoreAccessor(Scorable scorer) {
        this.scorer = scorer;
    }

    float score() {
        try {
            return scorer.score();
        } catch (IOException e) {
            throw new RuntimeException("Could not get score", e);
        }
    }

    @Override
    public int intValue() {
        return (int) score();
    }

    @Override
    public long longValue() {
        return (long) score();
    }

    @Override
    public float floatValue() {
        return score();
    }

    @Override
    public double doubleValue() {
        return score();
    }
}
