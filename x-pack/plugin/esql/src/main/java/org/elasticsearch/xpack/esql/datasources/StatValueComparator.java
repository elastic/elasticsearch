/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;

/**
 * Type-normalizing comparator for column-statistic values (min/max) against query literals.
 * <p>
 * Source plugins box an INT column's min/max with whatever Java width their reader exposes
 * (ORC's {@code IntegerColumnStatistics.getMinimum()} returns {@code long}, so the value
 * autoboxes to {@link Long}; parquet-mr returns {@code int}, so it boxes to {@link Integer}).
 * The ES|QL parser, meanwhile, fits small integer literals to {@link Integer}. A raw
 * {@code Comparable.compareTo} between two different boxed numeric types throws
 * {@link ClassCastException} (e.g. {@code Long.compareTo(Integer)}), so the planner must
 * normalize types before comparing rather than relying on each source plugin to box stats
 * in a planner-expected width.
 */
public final class StatValueComparator {

    /**
     * Returned by {@link #compare(Object, Object)} when the two values cannot be meaningfully
     * compared (null operand, or differing non-numeric runtime types). Callers must treat this
     * as "unknown" and fall back to a conservative decision rather than acting on the value.
     */
    public static final int INCOMPARABLE = Integer.MIN_VALUE;

    private StatValueComparator() {}

    /**
     * Compares two stat/literal values, returning negative, zero, or positive as with
     * {@link Comparable#compareTo}, or {@link #INCOMPARABLE} when the values are not comparable
     * (type mismatch or null). Uses exact long comparison for integral types to avoid double
     * precision loss, and widens to double for mixed integral/floating comparisons.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static int compare(Object a, Object b) {
        if (a == null || b == null) {
            return INCOMPARABLE;
        }
        if (a instanceof BytesRef br) {
            a = br.utf8ToString();
        }
        if (b instanceof BytesRef br) {
            b = br.utf8ToString();
        }
        if (a instanceof Number na && b instanceof Number nb) {
            if (isIntegral(na) && isIntegral(nb)) {
                return Long.compare(na.longValue(), nb.longValue());
            }
            return Double.compare(na.doubleValue(), nb.doubleValue());
        }
        if (a instanceof Comparable ca && a.getClass() == b.getClass()) {
            return ca.compareTo(b);
        }
        return INCOMPARABLE;
    }

    private static boolean isIntegral(Number n) {
        return n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte;
    }
}
