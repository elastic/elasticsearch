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
        if (a instanceof Number na && b instanceof Number nb) {
            if (isIntegral(na) && isIntegral(nb)) {
                return Long.compare(na.longValue(), nb.longValue());
            }
            double da = na.doubleValue();
            double db = nb.doubleValue();
            // ES|QL evaluators compare primitives, where IEEE 754 equates -0.0 and 0.0; Double.compare
            // orders -0.0 first. A MISS on opposite-sign zeros would drop splits whose every row matches.
            if (da == 0.0 && db == 0.0) {
                return 0;
            }
            return Double.compare(da, db);
        }
        // Keyword/string extrema (text harvest emits BytesRef, parquet footer emits String) are ordered by the
        // single shared UTF-8 keyword comparator so a filtered MATCH/MISS classification never diverges from a
        // scan (see SourceStatisticsSerializer.compareKeywordUtf8 for the UTF-16-vs-UTF-8 rationale).
        boolean aStr = a instanceof BytesRef || a instanceof String;
        boolean bStr = b instanceof BytesRef || b instanceof String;
        if (aStr && bStr) {
            return SourceStatisticsSerializer.compareKeywordUtf8(a, b);
        }
        if (a instanceof Comparable ca && a.getClass() == b.getClass()) {
            return ca.compareTo(b);
        }
        return INCOMPARABLE;
    }

    /**
     * {@code true} when both values are numeric zeros of opposite sign. The engine's {@code IN} uses
     * {@link Double#compare}, so unlike {@code ==} it tells {@code -0.0} from {@code 0.0}. Callers that
     * must not change that answer (classifier {@code IN}, partition {@code IN}) leave the pair unknown.
     */
    public static boolean zerosOfOppositeSign(Object a, Object b) {
        return a instanceof Number na
            && b instanceof Number nb
            && na.doubleValue() == 0.0
            && nb.doubleValue() == 0.0
            && Double.compare(na.doubleValue(), nb.doubleValue()) != 0;
    }

    private static boolean isIntegral(Number n) {
        return n instanceof Long || n instanceof Integer || n instanceof Short || n instanceof Byte;
    }
}
