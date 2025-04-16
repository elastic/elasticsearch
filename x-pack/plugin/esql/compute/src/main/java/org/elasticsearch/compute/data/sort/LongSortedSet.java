/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.sort;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.sort.SortOrder;

public class LongSortedSet implements Releasable {
    private final SortOrder order;
    private final boolean nullsFirst;

    private final LongArray values;
    private boolean hasNull;
    private int valuesSize;

    public LongSortedSet(BigArrays bigArrays, SortOrder order, boolean nullsFirst, int limit) {
        this.order = order;
        this.nullsFirst = nullsFirst;

        this.values = bigArrays.newLongArray(limit, false);
        this.hasNull = false;
        this.valuesSize = 0;
    }

    public boolean add(long value) {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public void close() {
        values.close();
    }
}
