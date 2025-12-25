/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.core.RefCounted;

// FIXME(gal, NOCOMMIT) document
final class GroupedRow implements Row {
    private final CircuitBreaker breaker;
    private final UngroupedRow row;
    private final BreakingBytesRefBuilder groupKey;

    GroupedRow(UngroupedRow row, CircuitBreaker breaker, int preAllocatedGroupKeySize) {
        this.breaker = breaker;
        this.row = row;
        this.groupKey = new BreakingBytesRefBuilder(breaker, "topn", preAllocatedGroupKeySize);
    }

    // GroupedRow(UngroupedRow row, CircuitBreaker breaker, BreakingBytesRefBuilder groupKey) {
    // this.row = row;
    // this.groupKey = groupKey;
    // }

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
        return row.ramBytesUsed() + groupKey.ramBytesUsed();
    }

    @Override
    public void close() {
        row.close();
        groupKey.close();
    }
}
