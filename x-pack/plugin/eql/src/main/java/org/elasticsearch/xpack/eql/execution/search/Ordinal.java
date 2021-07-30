/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import java.util.Objects;

public class Ordinal implements Comparable<Ordinal>, Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Ordinal.class);

    private final long timestamp;
    private final Comparable<Object> tiebreaker;
    private final long implicitTiebreaker; // _shard_doc tiebreaker automatically added by ES PIT

    public Ordinal(long timestamp, Comparable<Object> tiebreaker, long implicitTiebreaker) {
        this.timestamp = timestamp;
        this.tiebreaker = tiebreaker;
        this.implicitTiebreaker = implicitTiebreaker;
    }

    public long timestamp() {
        return timestamp;
    }

    public Comparable<Object> tiebreaker() {
        return tiebreaker;
    }

    public long implicitTiebreaker() {
        return implicitTiebreaker;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, tiebreaker, implicitTiebreaker);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Ordinal other = (Ordinal) obj;
        return Objects.equals(timestamp, other.timestamp)
                && Objects.equals(tiebreaker, other.tiebreaker)
                && Objects.equals(implicitTiebreaker, other.implicitTiebreaker);
    }

    @Override
    public String toString() {
        return "[" + timestamp + "][" + (tiebreaker != null ? tiebreaker.toString() : "") + "][" + implicitTiebreaker + "]";
    }

    @Override
    public int compareTo(Ordinal o) {
        if (timestamp < o.timestamp) {
            return -1;
        }
        if (timestamp == o.timestamp) {
            if (tiebreaker != null) {
                if (o.tiebreaker != null) {
                    if (tiebreaker.compareTo(o.tiebreaker) == 0) {
                        return Long.compare(implicitTiebreaker, o.implicitTiebreaker);
                    }
                    return tiebreaker.compareTo(o.tiebreaker);
                } else {
                    return -1;
                }
            }
            // this tiebreaker is null
            else {
                // nulls are last so unless both are null (equal)
                // this ordinal is greater (after) then the other tiebreaker
                // so fall through to 1
                if (o.tiebreaker == null) {
                    return Long.compare(implicitTiebreaker, o.implicitTiebreaker);
                }
            }
        }
        // if none of the branches above matched, this ordinal is greater than o
        return 1;
    }

    public boolean between(Ordinal left, Ordinal right) {
        return (compareTo(left) <= 0 && compareTo(right) >= 0) || (compareTo(right) <= 0 && compareTo(left) >= 0);
    }

    public boolean before(Ordinal other) {
        return compareTo(other) < 0;
    }

    public boolean beforeOrAt(Ordinal other) {
        return compareTo(other) <= 0;
    }

    public boolean after(Ordinal other) {
        return compareTo(other) > 0;
    }

    public boolean afterOrAt(Ordinal other) {
        return compareTo(other) >= 0;
    }

    public Object[] toArray() {
        return tiebreaker != null ?
            new Object[] { timestamp, tiebreaker, implicitTiebreaker } 
            : new Object[] { timestamp, implicitTiebreaker };
    }
}
