/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.elasticsearch.search.SearchHit;

import java.util.Objects;

/**
 * A match within a sequence, holding the result and occurrance time.
 */
class Match {

    private final long timestamp;
    private final Comparable<Object> tieBreaker;
    private final SearchHit hit;

    Match(long timestamp, Comparable<Object> tieBreaker, SearchHit hit) {
        this.timestamp = timestamp;
        this.tieBreaker = tieBreaker;
        this.hit = hit;
    }

    long timestamp() {
        return timestamp;
    }

    Comparable<Object> tieBreaker() {
        return tieBreaker;
    }

    SearchHit hit() {
        return hit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, tieBreaker, hit);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Match other = (Match) obj;
        return Objects.equals(timestamp, other.timestamp)
                && Objects.equals(tieBreaker, other.tieBreaker)
                && Objects.equals(hit, other.hit);
    }

    @Override
    public String toString() {
        return timestamp + "[" + (tieBreaker != null ? tieBreaker : "") + "]->" + hit.getId();
    }
}
