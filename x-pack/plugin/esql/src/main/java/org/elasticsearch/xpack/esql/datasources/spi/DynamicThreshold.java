/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.operator.topn.SharedNumericThreshold;
import org.elasticsearch.core.Releasable;

/**
 * Per-query descriptor for the live numeric TopN threshold exposed to format readers.
 */
public final class DynamicThreshold implements Releasable {
    private final String columnName;
    private final ElementType elementType;
    private final boolean ascending;
    private final boolean nullsFirst;
    private final SharedNumericThreshold channel;

    public DynamicThreshold(
        String columnName,
        ElementType elementType,
        boolean ascending,
        boolean nullsFirst,
        SharedNumericThreshold channel
    ) {
        this.columnName = columnName;
        this.elementType = elementType;
        this.ascending = ascending;
        this.nullsFirst = nullsFirst;
        this.channel = channel;
    }

    public String columnName() {
        return columnName;
    }

    public ElementType elementType() {
        return elementType;
    }

    public boolean nullsFirst() {
        return nullsFirst;
    }

    public boolean dominates(long rangeMin, long rangeMax) {
        if (channel.noFurtherCandidates()) {
            return true;
        }
        return channel.dominates(rangeMin, rangeMax);
    }

    public boolean dominates(long rangeMin, long rangeMax, long nullCount) {
        if (channel.noFurtherCandidates()) {
            return true;
        }
        if (nullCount > 0 && nullsFirst) {
            return false;
        }
        return channel.dominates(rangeMin, rangeMax);
    }

    public boolean dominatesNulls(long nullCount) {
        if (channel.noFurtherCandidates()) {
            return true;
        }
        return nullCount > 0 && nullsFirst == false && hasBound();
    }

    public boolean noFurtherCandidates() {
        return channel.noFurtherCandidates();
    }

    public long current() {
        return channel.current();
    }

    public boolean hasBound() {
        return ascending ? current() != Long.MAX_VALUE : current() != Long.MIN_VALUE;
    }

    @Override
    public void close() {
        channel.close();
    }
}
