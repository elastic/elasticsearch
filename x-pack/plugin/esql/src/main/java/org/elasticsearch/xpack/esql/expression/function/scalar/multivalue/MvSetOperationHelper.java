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

    public static class OperationalSet<E> extends LinkedHashSet<E> {

        /**
         * Performs an in-place union with the given set.
         * Adds all elements from the given set to this set.
         * @param set the set to union with
         * @return this set after the union operation
         */
        public Set<E> union(Set<E> set) {
            this.addAll(set);
            return this;
        }

        /**
         * Performs an in-place intersection with the given set.
         * Retains only elements that are present in both sets.
         * @param set the set to intersect with
         * @return this set after the intersection operation
         */
        public Set<E> intersect(Set<E> set) {
            this.retainAll(set);
            return this;
        }
    }
}
