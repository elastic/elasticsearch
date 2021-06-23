/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

public class SequenceKey implements Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(SequenceKey.class);

    public static final SequenceKey NONE = new SequenceKey();

    private final Object[] keys;
    private final int hashCode;

    SequenceKey(Object... keys) {
        this.keys = keys;
        this.hashCode = Objects.hash(keys);
    }

    public List<Object> asList() {
        return keys == null ? emptyList() : Arrays.asList(keys);
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + RamUsageEstimator.sizeOfObject(keys);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SequenceKey other = (SequenceKey) obj;
        return Arrays.equals(keys, other.keys);
    }

    @Override
    public String toString() {
        return CollectionUtils.isEmpty(keys) ? "NONE" : Arrays.toString(keys);
    }
}
