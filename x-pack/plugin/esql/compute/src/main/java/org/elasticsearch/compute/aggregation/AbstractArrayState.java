/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

public class AbstractArrayState implements Releasable {
    protected final BigArrays bigArrays;

    private BitArray seen;

    public AbstractArrayState(BigArrays bigArrays) {
        this.bigArrays = bigArrays;
    }

    final boolean hasValue(int groupId) {
        return seen == null || seen.get(groupId);
    }

    /**
     * Switches this array state into tracking which group ids are set. This is
     * idempotent and fast if already tracking so it's safe to, say, call it once
     * for every block of values that arrives containing {@code null}.
     */
    final void enableGroupIdTracking(SeenGroupIds seenGroupIds) {
        if (seen == null) {
            seen = seenGroupIds.seenGroupIds(bigArrays);
        }
    }

    protected final void trackGroupId(int groupId) {
        if (trackingGroupIds()) {
            seen.set(groupId);
        }
    }

    protected final boolean trackingGroupIds() {
        return seen != null;
    }

    @Override
    public void close() {
        Releasables.close(seen);
    }
}
