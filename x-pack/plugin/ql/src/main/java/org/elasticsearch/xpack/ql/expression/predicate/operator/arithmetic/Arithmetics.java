/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

import java.math.BigInteger;
import java.util.function.BiFunction;

import static org.elasticsearch.xpack.ql.util.Check.isUnsignedLong;

/**
 * Arithmetic operation using the type widening rules of the JLS 5.6.2 namely
 * widen to double or float or long or int in this order.
 */
public final class Arithmetics {

    private Arithmetics() {}

    public interface NumericArithmetic extends BiFunction<Number, Number, Number> {
        default Object wrap(Object l, Object r) {
            if (!(l instanceof Number)) {
                throw new QlIllegalArgumentException("A number is required; received {}", l);
            }

            if (!(r instanceof Number)) {
                throw new QlIllegalArgumentException("A number is required; received {}", r);
            }

            return apply((Number) l, (Number) r);
        }
    }

    private static BigInteger unsignedLongOperation(Number l, Number r, BiFunction<BigInteger, BigInteger, BigInteger> op) {
        BigInteger biLeft, biRight;
        if (l instanceof BigInteger) {
            biLeft = (BigInteger) l;
            biRight = BigInteger.valueOf(r.longValue());
        } else if (r instanceof BigInteger) {
            biLeft = BigInteger.valueOf(l.longValue());
            biRight = (BigInteger) r;
        } else {
            return null;
        }
        BigInteger bi = op.apply(biLeft, biRight);
        isUnsignedLong(bi);
        return bi;
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
        BigInteger bi = unsignedLongOperation(l, r, BigInteger::add);
        if (bi != null) {
            return bi;
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
        BigInteger bi = unsignedLongOperation(l, r, BigInteger::subtract);
        if (bi != null) {
            return bi;
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
        // Note: in case of unsigned_long overflow (or underflow, with negative fixed point numbers), the exception is thrown.
        // This is unlike the way some other traditional RDBMS that support unsigned types work, which simply promote the result to a
        // floating point type, but in line with how our implementation treats other fixed point type operations (i.e. Math#xxExact()).
        // The reason for our behavior is (prolly) the need to establish a schema based on an index mapping. Noteworthy however is also
        // that we're not strictly consistent with the returned type: a `SUM(field)` aggregation can return a floating point, for example.
        // TODO: document this last point or address it?
        BigInteger bi = unsignedLongOperation(l, r, BigInteger::multiply);
        if (bi != null) {
            return bi;
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
        BigInteger bi = unsignedLongOperation(l, r, BigInteger::divide);
        if (bi != null) {
            return bi;
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
        BigInteger bi = unsignedLongOperation(l, r, BigInteger::remainder);
        if (bi != null) {
            return bi;
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
                throw new ArithmeticException("unsigned_long overflow");
            }
            return n;
        }
        if (n instanceof Long) {
            return Long.valueOf(Math.negateExact(n.longValue()));
        }

        return Integer.valueOf(Math.negateExact(n.intValue()));
    }
}
