/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.common.search.aggregations;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.core.Releasable;

/**
 * Helps long-valued {@link org.elasticsearch.search.sort.BucketedSort.ExtraData} track "empty" slots. It attempts to have
 * very low CPU overhead and no memory overhead when there *aren't* empty
 * values.
 */
public class MissingHelper implements Releasable {
    private final BigArrays bigArrays;
    private BitArray tracker;

    public MissingHelper(BigArrays bigArrays) {
        this.bigArrays = bigArrays;
    }

    public void markMissing(long index) {
        if (tracker == null) {
            tracker = new BitArray(index, bigArrays);
        }
        tracker.set(index);
    }

    public void markNotMissing(long index) {
        if (tracker == null) {
            return;
        }
        tracker.clear(index);
    }

    public void swap(long lhs, long rhs) {
        if (tracker == null) {
            return;
        }
        boolean backup = tracker.get(lhs);
        if (tracker.get(rhs)) {
            tracker.set(lhs);
        } else {
            tracker.clear(lhs);
        }
        if (backup) {
            tracker.set(rhs);
        } else {
            tracker.clear(rhs);
        }
    }

    public boolean isEmpty(long index) {
        if (tracker == null) {
            return false;
        }
        return tracker.get(index);
    }

    @Override
    public void close() {
        if (tracker != null) {
            tracker.close();
        }
    }
}
