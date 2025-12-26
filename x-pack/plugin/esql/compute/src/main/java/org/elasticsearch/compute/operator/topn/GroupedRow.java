/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;

// FIXME(gal, NOCOMMIT) document
final class GroupedRow implements Row {
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(GroupedRow.class);
    private final UngroupedRow row;
    private final BreakingBytesRefBuilder groupKey;

    GroupedRow(UngroupedRow row, CircuitBreaker breaker, int preAllocatedGroupKeySize) {
        breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, "topn");
        this.row = row;
        boolean success = false;
        try {
            this.groupKey = new BreakingBytesRefBuilder(breaker, "topn", preAllocatedGroupKeySize);
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    public BreakingBytesRefBuilder groupKey() {
        return groupKey;
    }

    @Override
    public BreakingBytesRefBuilder keys() {
        return row.keys();
    }

    @Override
    public TopNOperator.BytesOrder bytesOrder() {
        return row.bytesOrder();
    }

    @Override
    public void setShardRefCounted(RefCounted refCounted) {
        row.setShardRefCounted(refCounted);
    }

    @Override
    public BreakingBytesRefBuilder values() {
        return row.values();
    }

    @Override
    public void clear() {
        row.clear();
        groupKey.clear();
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + row.ramBytesUsed() + groupKey.ramBytesUsed() + 16;
    }

    @Override
    public void close() {
        row.close();
        Releasables.closeExpectNoException(groupKey);
    }

    public void writeGroupKey(int i, Row row) {
        throw new AssertionError("TODO(gal) NOCOMMIT");
    }
}
