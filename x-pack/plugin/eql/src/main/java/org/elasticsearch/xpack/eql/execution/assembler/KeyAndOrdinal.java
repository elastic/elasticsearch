/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.assembler;

import org.elasticsearch.xpack.eql.execution.sequence.SequenceKey;

import java.util.Objects;

class KeyAndOrdinal {
    final SequenceKey key;
    final long timestamp;
    final Comparable<Object> tiebreaker;

    KeyAndOrdinal(SequenceKey key, long timestamp, Comparable<Object> tiebreaker) {
        this.key = key;
        this.timestamp = timestamp;
        this.tiebreaker = tiebreaker;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, timestamp, tiebreaker);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        KeyAndOrdinal other = (KeyAndOrdinal) obj;
        return Objects.equals(key, other.key)
                && Objects.equals(timestamp, other.timestamp)
                && Objects.equals(tiebreaker, other.tiebreaker);
    }

    @Override
    public String toString() {
        return key + "[" + timestamp + "][" + (tiebreaker != null ? Objects.toString(tiebreaker) : "") + "]";
    }
}
