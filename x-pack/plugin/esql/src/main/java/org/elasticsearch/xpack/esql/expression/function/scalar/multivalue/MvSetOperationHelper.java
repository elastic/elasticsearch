/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Shared set operations for MV functions.
 * Preserves insertion order using LinkedHashSet.
 */
public final class MvSetOperationHelper {

    private MvSetOperationHelper() {}

    /**
     * Union: returns all unique elements from both sets.
     * Order: elements from set1 first, then new elements from set2.
     */
    public static <T> Set<T> union(Set<T> set1, Set<T> set2) {
        Set<T> result = new LinkedHashSet<>(set1);
        result.addAll(set2);
        return result;
    }

    /**
     * Intersection: returns elements present in both sets.
     * Order: preserved from set1.
     */
    public static <T> Set<T> intersection(Set<T> set1, Set<T> set2) {
        Set<T> result = new LinkedHashSet<>(set1);
        result.retainAll(set2);
        return result;
    }
}
