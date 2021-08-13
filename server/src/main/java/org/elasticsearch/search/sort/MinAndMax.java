/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.Comparator;
import java.util.Objects;

/**
 * A class that encapsulates a minimum and a maximum, that are of the same type and {@link Comparable}.
 */
public class MinAndMax<T extends Comparable<? super T>> implements Writeable {
    private final T minValue;
    private final T maxValue;

    public MinAndMax(T minValue, T maxValue) {
        this.minValue = Objects.requireNonNull(minValue);
        this.maxValue = Objects.requireNonNull(maxValue);
    }

    @SuppressWarnings("unchecked")
    public MinAndMax(StreamInput in) throws IOException {
        this.minValue = (T)Lucene.readSortValue(in);
        this.maxValue = (T)Lucene.readSortValue(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Lucene.writeSortValue(out, minValue);
        Lucene.writeSortValue(out, maxValue);
    }

    /**
     * Return the minimum value.
     */
    public T getMin() {
        return minValue;
    }

    /**
     * Return the maximum value.
     */
    public T getMax() {
        return maxValue;
    }

    /**
     * Return a {@link Comparator} for {@link MinAndMax} values according to the provided {@link SortOrder}.
     */
    public static <T extends Comparable<? super T>> Comparator<MinAndMax<T>> getComparator(SortOrder order) {
        Comparator<MinAndMax<T>> cmp = order == SortOrder.ASC  ?
            Comparator.comparing(MinAndMax::getMin) : Comparator.comparing(MinAndMax::getMax);
        if (order == SortOrder.DESC) {
            cmp = cmp.reversed();
        }
        return Comparator.nullsLast(cmp);
    }
}
