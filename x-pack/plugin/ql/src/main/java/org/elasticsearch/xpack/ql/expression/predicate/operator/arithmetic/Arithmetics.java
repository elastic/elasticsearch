/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

import java.math.BigInteger;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.ql.util.NumericUtils.asUnsignedLong;

/**
 * Arithmetic operation using the type widening rules of the JLS 5.6.2 namely
 * widen to double or float or long or int in this order.
 */
public final class Arithmetics {

    private Arithmetics() {}

    public interface NumericArithmetic extends BiFunction<Number, Number, Number> {
        default Object wrap(Object l, Object r) {
            if ((l instanceof Number) == false) {
                throw new QlIllegalArgumentException("A number is required; received {}", l);
            }

            if ((r instanceof Number) == false) {
                throw new QlIllegalArgumentException("A number is required; received {}", r);
            }

            return apply((Number) l, (Number) r);
        }
    }

    public static Number add(Number l, Number r) {
        if (l == null || r == null) {
            return null;
        }

        if (l instanceof Double || r instanceof Double) {
            return Double.valueOf(l.doubleValue() + r.doubleValue());
        }
        if (l instanceof Float || r instanceof Float) {
            return Float.valueOf(l.floatValue() + r.floatValue());
        }
        if (l instanceof BigInteger || r instanceof BigInteger) {
            BigInteger bi = asBigInteger(l).add(asBigInteger(r));
            return asUnsignedLong(bi);
        }
        if (l instanceof Long || r instanceof Long) {
            return Long.valueOf(Math.addExact(l.longValue(), r.longValue()));
        }

        return Integer.valueOf(Math.addExact(l.intValue(), r.intValue()));
    }

    public static Number sub(Number l, Number r) {
        if (l == null || r == null) {
            return null;
        }

        if (l instanceof Double || r instanceof Double) {
            return Double.valueOf(l.doubleValue() - r.doubleValue());
        }
        if (l instanceof Float || r instanceof Float) {
            return Float.valueOf(l.floatValue() - r.floatValue());
        }
        if (l instanceof BigInteger || r instanceof BigInteger) {
            BigInteger bi = asBigInteger(l).subtract(asBigInteger(r));
            return asUnsignedLong(bi);
        }
        if (l instanceof Long || r instanceof Long) {
            return Long.valueOf(Math.subtractExact(l.longValue(), r.longValue()));
        }

        return Integer.valueOf(Math.subtractExact(l.intValue(), r.intValue()));
    }

    public static Number mul(Number l, Number r) {
        if (l == null || r == null) {
            return null;
        }

        if (l instanceof Double || r instanceof Double) {
            return Double.valueOf(l.doubleValue() * r.doubleValue());
        }
        if (l instanceof Float || r instanceof Float) {
            return Float.valueOf(l.floatValue() * r.floatValue());
        }
        if (l instanceof BigInteger || r instanceof BigInteger) {
            BigInteger bi = asBigInteger(l).multiply(asBigInteger(r));
            // Note: in case of unsigned_long overflow (or underflow, with negative fixed point numbers), the exception is thrown.
            // This is unlike the way some other traditional RDBMS that support unsigned types work, which simply promote the result to a
            // floating point type, but in line with how our implementation treats other fixed point type operations (i.e. Math#xxExact()).
            return asUnsignedLong(bi);
        }
        if (l instanceof Long || r instanceof Long) {
            return Long.valueOf(Math.multiplyExact(l.longValue(), r.longValue()));
        }

        return Integer.valueOf(Math.multiplyExact(l.intValue(), r.intValue()));
    }

    public static Number div(Number l, Number r) {
        if (l == null || r == null) {
            return null;
        }

        if (l instanceof Double || r instanceof Double) {
            return l.doubleValue() / r.doubleValue();
        }
        if (l instanceof Float || r instanceof Float) {
            return l.floatValue() / r.floatValue();
        }
        if (l instanceof BigInteger || r instanceof BigInteger) {
            BigInteger bi = asBigInteger(l).divide(asBigInteger(r));
            return asUnsignedLong(bi);
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
        if (l instanceof BigInteger || r instanceof BigInteger) {
            BigInteger bi = asBigInteger(l).remainder(asBigInteger(r));
            return asUnsignedLong(bi);
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
        if (n instanceof BigInteger) {
            if (((BigInteger) n).signum() != 0) {
                throw new ArithmeticException("unsigned_long overflow"); // in the scope of the unsigned_long type
            }
            return n;
        }
        if (n instanceof Long) {
            return Long.valueOf(Math.negateExact(n.longValue()));
        }

        return Integer.valueOf(Math.negateExact(n.intValue()));
    }

    public static BigInteger asBigInteger(Number n) {
        return n instanceof BigInteger ? (BigInteger) n : BigInteger.valueOf(n.longValue());
    }
}
