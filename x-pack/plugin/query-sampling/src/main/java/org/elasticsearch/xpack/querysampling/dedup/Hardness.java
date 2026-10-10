/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

/**
 * How hard a query is for the index to answer, in thirds of the queries of its field.
 */
public enum Hardness {
    /** The first hits are far better than the last: the neighbourhood is sparse and easy to tell apart. */
    EASY(-1),
    MEDIUM(0),
    /** The hits are all about as good: the neighbourhood is crowded and hard to tell apart. */
    HARD(1);

    private final int rank;

    Hardness(int rank) {
        this.rank = rank;
    }

    /**
     * -1, 0 or 1, which is what the tilt of the probabilities is applied to.
     */
    public int rank() {
        return rank;
    }
}
