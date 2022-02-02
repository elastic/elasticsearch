/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.comparison;

import java.util.Set;

/**
 * Comparison utilities.
 */
public final class Comparisons {

    private Comparisons() {}

    public static Boolean eq(Object l, Object r) {
        Integer i = compare(l, r);
        return i == null ? null : i.intValue() == 0;
    }

    public static boolean nulleq(Object l, Object r) {
        if (l == null && r == null) {
            return true;
        }
        Integer i = compare(l, r);
        return i == null ? false : i.intValue() == 0;
    }

    static Boolean neq(Object l, Object r) {
        Integer i = compare(l, r);
        return i == null ? null : i.intValue() != 0;
    }

    public static Boolean lt(Object l, Object r) {
        Integer i = compare(l, r);
        return i == null ? null : i.intValue() < 0;
    }

    static Boolean lte(Object l, Object r) {
        Integer i = compare(l, r);
        return i == null ? null : i.intValue() <= 0;
    }

    public static Boolean gt(Object l, Object r) {
        Integer i = compare(l, r);
        return i == null ? null : i.intValue() > 0;
    }

    static Boolean gte(Object l, Object r) {
        Integer i = compare(l, r);
        return i == null ? null : i.intValue() >= 0;
    }

    static Boolean in(Object l, Set<Object> r) {
        return r.contains(l);
    }

    /**
     * Compares two expression arguments (typically Numbers), if possible.
     * Otherwise returns null (the arguments are not comparable or at least
     * one of them is null).
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    static Integer compare(Object l, Object r) {
        if (l == null || r == null) {
            return null;
        }
        // typical number comparison
        if (l instanceof Number && r instanceof Number) {
            return compare((Number) l, (Number) r);
        }

        if (l instanceof Comparable && r instanceof Comparable) {
            try {
                return Integer.valueOf(((Comparable) l).compareTo(r));
            } catch (ClassCastException cce) {
                // when types are not compatible, cce is thrown
                // fall back to null
                return null;
            }
        }

        return null;
    }

    private static Integer compare(Number l, Number r) {
        if (l instanceof Double || r instanceof Double) {
            return Double.compare(l.doubleValue(), r.doubleValue());
        }
        if (l instanceof Float || r instanceof Float) {
            return Float.compare(l.floatValue(), r.floatValue());
        }
        if (l instanceof Long || r instanceof Long) {
            return Long.compare(l.longValue(), r.longValue());
        }

        return Integer.valueOf(Integer.compare(l.intValue(), r.intValue()));
    }
}
