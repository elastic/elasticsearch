/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.elasticsearch.xpack.eql.execution.search.HitReference;
import org.elasticsearch.xpack.eql.execution.search.Ordinal;

import java.util.Objects;

/**
 * A match within a sequence, holding the result and occurrance time.
 */
class Match {

    private final Ordinal ordinal;
    private final HitReference hit;

    Match(Ordinal ordinal, HitReference hit) {
        this.ordinal = ordinal;
        this.hit = hit;
    }

    Ordinal ordinal() {
        return ordinal;
    }

    HitReference hit() {
        return hit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ordinal, hit);
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
        return Objects.equals(ordinal, other.ordinal)
                && Objects.equals(hit, other.hit);
    }

    @Override
    public String toString() {
        return ordinal + "->" + hit;
    }
}
