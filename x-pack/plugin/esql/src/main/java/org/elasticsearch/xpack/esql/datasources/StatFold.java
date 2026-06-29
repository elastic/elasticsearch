/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

/**
 * The fold law of a statistic: how two values of the same kind combine into one.
 *
 * <p>Every {@link #apply} is associative and commutative, so a folded result is invariant to the
 * order, the source query, and the node that produced the parts — "who wrote it doesn't matter".
 * This is the one place the law lives: the values themselves stay plain (the column's raw
 * min/max/count), and the law for a given statistic is selected once by {@link StatFolds#foldFor},
 * never re-implemented at a combine site.
 *
 * <p>{@code MIN}/{@code MAX} delegate to {@link SplitStats#mergedMin}/{@link SplitStats#mergedMax},
 * which already widen compatible numerics and return {@code null} for incompatible inputs; that null
 * (and any null or wrong-typed input) becomes {@link #POISON}, which propagates and forces a
 * downstream safe-miss. The fold needs no type tag on the value — incompatibility is detected by the
 * merge itself.
 */
enum StatFold {
    MIN {
        @Override
        Object apply(Object a, Object b) {
            return extremum(a, b, true);
        }

        @Override
        Object first(Object value) {
            return value instanceof Comparable ? value : POISON;
        }
    },
    MAX {
        @Override
        Object apply(Object a, Object b) {
            return extremum(a, b, false);
        }

        @Override
        Object first(Object value) {
            return value instanceof Comparable ? value : POISON;
        }
    },
    SUM {
        @Override
        Object apply(Object a, Object b) {
            if (a == POISON || b == POISON) {
                return POISON;
            }
            if (a instanceof Number na && b instanceof Number nb) {
                if (a instanceof Double || b instanceof Double || a instanceof Float || b instanceof Float) {
                    return na.doubleValue() + nb.doubleValue();
                }
                return na.longValue() + nb.longValue();
            }
            return POISON;
        }

        @Override
        Object first(Object value) {
            // Normalise an integral count/size to long (matching the prior accumulator's long seed), so a
            // lone contributor emits the same type as a folded one; a non-number cannot be served.
            if (value instanceof Double || value instanceof Float) {
                return ((Number) value).doubleValue();
            }
            return value instanceof Number n ? n.longValue() : POISON;
        }
    };

    /** Sentinel for a value that cannot be served (incompatible/ill-typed); forces a safe-miss downstream. */
    static final Object POISON = new Object();

    /** Combines two already-accepted values of this statistic; returns {@link #POISON} if they cannot combine. */
    abstract Object apply(Object a, Object b);

    /** Accepts the first contributor of this statistic, normalising/validating it; {@link #POISON} if unservable. */
    abstract Object first(Object value);

    private static Object extremum(Object a, Object b, boolean min) {
        if (a == POISON || b == POISON) {
            return POISON;
        }
        Object merged = min ? SplitStats.mergedMin(a, b) : SplitStats.mergedMax(a, b);
        return merged == null ? POISON : merged;
    }
}
