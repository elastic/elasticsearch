/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic;

import java.time.Duration;
import java.time.OffsetTime;
import java.time.Period;
import java.time.temporal.Temporal;

import static org.elasticsearch.xpack.sql.util.DateUtils.DAY_IN_MILLIS;

/**
 * Arithmetic operation using the type widening rules of the JLS 5.6.2 namely
 * widen to double or float or long or int in this order.
 */
public final class Arithmetics {

    private Arithmetics() {}

    private enum IntervalOperation {
        ADD,
        SUB
    }

    static Number add(Number l, Number r) {
        if (l == null || r == null) {
            return null;
        }

        if (l instanceof Double || r instanceof Double) {
            return Double.valueOf(l.doubleValue() + r.doubleValue());
        }
        if (l instanceof Float || r instanceof Float) {
            return Float.valueOf(l.floatValue() + r.floatValue());
        }
        if (l instanceof Long || r instanceof Long) {
            return Long.valueOf(Math.addExact(l.longValue(), r.longValue()));
        }

        return Integer.valueOf(Math.addExact(l.intValue(), r.intValue()));
    }

    static Temporal add(Temporal l, Period r) {
        return periodArithmetics(l, r, IntervalOperation.ADD);
    }

    static Temporal add(Temporal l, Duration r) {
        return durationArithmetics(l, r, IntervalOperation.ADD);
    }

    static Number sub(Number l, Number r) {
        if (l == null || r == null) {
            return null;
        }

        if (l instanceof Double || r instanceof Double) {
            return Double.valueOf(l.doubleValue() - r.doubleValue());
        }
        if (l instanceof Float || r instanceof Float) {
            return Float.valueOf(l.floatValue() - r.floatValue());
        }
        if (l instanceof Long || r instanceof Long) {
            return Long.valueOf(Math.subtractExact(l.longValue(), r.longValue()));
        }

        return Integer.valueOf(Math.subtractExact(l.intValue(), r.intValue()));
    }

    static Temporal sub(Temporal l, Period r) {
        return periodArithmetics(l, r, IntervalOperation.SUB);
    }

    static Temporal sub(Temporal l, Duration r) {
        return durationArithmetics(l, r, IntervalOperation.SUB);
    }

    static Number mul(Number l, Number r) {
        if (l == null || r == null) {
            return null;
        }

        if (l instanceof Double || r instanceof Double) {
            return Double.valueOf(l.doubleValue() * r.doubleValue());
        }
        if (l instanceof Float || r instanceof Float) {
            return Float.valueOf(l.floatValue() * r.floatValue());
        }
        if (l instanceof Long || r instanceof Long) {
            return Long.valueOf(Math.multiplyExact(l.longValue(), r.longValue()));
        }

        return Integer.valueOf(Math.multiplyExact(l.intValue(), r.intValue()));
    }

    static Number div(Number l, Number r) {
        if (l == null || r == null) {
            return null;
        }

        if (l instanceof Double || r instanceof Double) {
            return l.doubleValue() / r.doubleValue();
        }
        if (l instanceof Float || r instanceof Float) {
            return l.floatValue() / r.floatValue();
        }
        if (l instanceof Long || r instanceof Long) {
            return l.longValue() / r.longValue();
        }

        return l.intValue() / r.intValue();
    }

    public static Number mod(Number l, Number r) {
        if (l == null || r == null) {
            return null;
        }

        if (l instanceof Double || r instanceof Double) {
            return Double.valueOf(l.doubleValue() % r.doubleValue());
        }
        if (l instanceof Float || r instanceof Float) {
            return Float.valueOf(l.floatValue() % r.floatValue());
        }
        if (l instanceof Long || r instanceof Long) {
            return Long.valueOf(l.longValue() % r.longValue());
        }

        return l.intValue() % r.intValue();
    }

    static Number negate(Number n) {
        if (n == null) {
            return null;
        }

        if (n instanceof Double) {
            double d = n.doubleValue();
            if (d == Double.MIN_VALUE) {
                throw new ArithmeticException("double overflow");
            }
            return Double.valueOf(-n.doubleValue());
        }
        if (n instanceof Float) {
            float f = n.floatValue();
            if (f == Float.MIN_VALUE) {
                throw new ArithmeticException("float overflow");
            }
            return Float.valueOf(-n.floatValue());
        }
        if (n instanceof Long) {
            return Long.valueOf(Math.negateExact(n.longValue()));
        }

        return Integer.valueOf(Math.negateExact(n.intValue()));
    }

    private static Temporal periodArithmetics(Temporal l, Period r, IntervalOperation operation) {
        if (l == null || r == null) {
            return null;
        }

        if (l instanceof OffsetTime) {
            return l;
        }

        if (operation == IntervalOperation.ADD) {
            return l.plus(r);
        } else {
            return l.minus(r);
        }
    }

    private static Temporal durationArithmetics(Temporal l, Duration r, IntervalOperation operation) {
        if (l == null || r == null) {
            return null;
        }

        if (l instanceof OffsetTime) {
            r = Duration.ofMillis(r.toMillis() % DAY_IN_MILLIS);
        }

        if (operation == IntervalOperation.ADD) {
            return l.plus(r);
        } else {
            return l.minus(r);
        }
    }
}
