/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.xpack.eql.execution.search.Ordinal;

import java.util.Objects;

public class KeyAndOrdinal implements Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(KeyAndOrdinal.class);

    final SequenceKey key;
    final Ordinal ordinal;

    public KeyAndOrdinal(SequenceKey key, Ordinal ordinal) {
        this.key = key;
        this.ordinal = ordinal;
    }

    public SequenceKey key() {
        return key;
    }

    public Ordinal ordinal() {
        return ordinal;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + RamUsageEstimator.sizeOf(key) + RamUsageEstimator.sizeOf(ordinal);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, ordinal);
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
                && Objects.equals(ordinal, other.ordinal);
    }

    @Override
    public String toString() {
        return key.toString() + ordinal.toString();
    }
}
