/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

/**
 * Tracks how many new fields may be added during a mapper merge, and enforces that limit.
 * Shared across all child {@link MapperMergeContext} instances created from a root context,
 * so that fields consumed by one child object mapper also decrement the budget for siblings.
 * <p>
 * Three modes:
 * <ul>
 *   <li>{@link #unlimited()} — no limit; used for recovery merges of already-validated mappings.</li>
 *   <li>{@link #dropping(long)} — silently drops fields once the budget is exhausted; used for
 *       auto-updates with {@code ignore_dynamic_beyond_limit}.</li>
 *   <li>{@link #throwing(long, long)} — throws {@link IllegalArgumentException} when the budget
 *       is exhausted; used for explicit mapping updates where exceeding the limit is an error.</li>
 * </ul>
 * Not thread-safe; the same instance must not be modified by multiple threads.
 */
public interface NewFieldsBudget {

    static NewFieldsBudget unlimited() {
        return Unlimited.INSTANCE;
    }

    static NewFieldsBudget dropping(long fieldsBudget) {
        if (fieldsBudget == Long.MAX_VALUE) {
            return Unlimited.INSTANCE;
        }
        return new Limited(fieldsBudget);
    }

    /**
     * Returns a budget that throws {@link IllegalArgumentException} when exhausted.
     *
     * @param remainingBudget fields remaining before the limit is hit (configured limit minus existing field count)
     * @param totalFieldsLimit the configured limit, used in the error message
     */
    static NewFieldsBudget throwing(long remainingBudget, long totalFieldsLimit) {
        return new Throwing(remainingBudget, totalFieldsLimit);
    }

    /**
     * Checks whether {@code fieldSize} fields can be added without exhausting the budget,
     * without modifying the budget. Use before speculatively building a mapper to avoid
     * unnecessary work when the budget is already exhausted.
     */
    boolean hasCapacityFor(long fieldSize);

    boolean decrementIfPossible(long fieldSize);

    final class Unlimited implements NewFieldsBudget {

        private static final Unlimited INSTANCE = new Unlimited();

        private Unlimited() {}

        @Override
        public boolean hasCapacityFor(long fieldSize) {
            return true;
        }

        @Override
        public boolean decrementIfPossible(long fieldSize) {
            return true;
        }
    }

    final class Limited implements NewFieldsBudget {

        private long fieldsBudget;

        Limited(long fieldsBudget) {
            this.fieldsBudget = fieldsBudget;
        }

        @Override
        public boolean hasCapacityFor(long fieldSize) {
            return fieldsBudget >= fieldSize;
        }

        @Override
        public boolean decrementIfPossible(long fieldSize) {
            if (fieldsBudget >= fieldSize) {
                fieldsBudget -= fieldSize;
                return true;
            }
            return false;
        }
    }

    final class Throwing implements NewFieldsBudget {

        private long fieldsBudget;
        private final long totalFieldsLimit;

        Throwing(long fieldsBudget, long totalFieldsLimit) {
            this.fieldsBudget = fieldsBudget;
            this.totalFieldsLimit = totalFieldsLimit;
        }

        @Override
        public boolean hasCapacityFor(long fieldSize) {
            return fieldsBudget >= fieldSize;
        }

        @Override
        public boolean decrementIfPossible(long fieldSize) {
            if (fieldsBudget >= fieldSize) {
                fieldsBudget -= fieldSize;
                return true;
            }
            throw new IllegalArgumentException("Limit of total fields [" + totalFieldsLimit + "] has been exceeded");
        }
    }
}
