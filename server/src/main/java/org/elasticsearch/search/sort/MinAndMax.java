/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
        this.minValue = (T) Lucene.readSortValue(in);
        this.maxValue = (T) Lucene.readSortValue(in);
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

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static final Comparator<MinAndMax> ASC_COMPARATOR = (left, right) -> {
        if (left == null) {
            return right == null ? 0 : -1; // nulls last
        }
        return right == null ? 1 : left.getMin().compareTo(right.getMin());
    };

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static final Comparator<MinAndMax> DESC_COMPARATOR = (left, right) -> {
        if (left == null) {
            return right == null ? 0 : 1; // nulls first
        }
        return right == null ? -1 : right.getMax().compareTo(left.getMax());
    };

    /**
     * Return a {@link Comparator} for {@link MinAndMax} values according to the provided {@link SortOrder}.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T extends Comparable<? super T>> Comparator<MinAndMax<T>> getComparator(SortOrder order) {
        return (Comparator) (order == SortOrder.ASC ? ASC_COMPARATOR : DESC_COMPARATOR);
    }
}
