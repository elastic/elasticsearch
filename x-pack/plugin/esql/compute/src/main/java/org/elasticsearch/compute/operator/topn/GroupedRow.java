/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;

// FIXME(gal, NOCOMMIT) document
final class GroupedRow implements Row {
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(GroupedRow.class);
    private final UngroupedRow row;
    // FIXME(gal, NOCOMMIT) This isn't actually needed, since we already maintain this in the hash table's key!
    private final BreakingBytesRefBuilder groupKey;

    GroupedRow(UngroupedRow row, int preAllocatedGroupKeySize) {
        this.row = row;
        // FIXME(gal, NOCOMMIT) There must be a better pattern for this...
        long assigned = 0;
        boolean success = false;
        try {
            row.breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, "topn");
            assigned += SHALLOW_SIZE;
            this.groupKey = new BreakingBytesRefBuilder(row.breaker, "topn", preAllocatedGroupKeySize);
            assigned += preAllocatedGroupKeySize;
            success = true;
        } finally {
            if (success == false) {
                row.close();
                row.breaker.addWithoutBreaking(-assigned);
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
        return SHALLOW_SIZE + row.ramBytesUsed() + groupKey.ramBytesUsed();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(() -> row.breaker.addWithoutBreaking(-SHALLOW_SIZE), groupKey);
        row.close();
    }
}
