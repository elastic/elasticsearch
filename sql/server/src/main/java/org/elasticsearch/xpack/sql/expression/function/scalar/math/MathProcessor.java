/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import java.util.function.DoubleFunction;
import java.util.function.Function;

/**
 * Applies a math function. Note that the order of the enum constants is used for serialization.
 */
public enum MathProcessor {
    ABS((Object l) -> {
        if (l instanceof Float) {
            return Math.abs(((Float) l).floatValue());
        }
        if (l instanceof Double) {
            return Math.abs(((Double) l).doubleValue());
        }
        long lo = ((Number) l).longValue();
        return lo >= 0 ? lo : lo == Long.MIN_VALUE ? Long.MAX_VALUE : -lo;
    }),
    ACOS(fromDouble(Math::acos)),
    ASIN(fromDouble(Math::asin)),
    ATAN(fromDouble(Math::atan)),
    CBRT(fromDouble(Math::cbrt)),
    CEIL(fromDouble(Math::ceil)),
    COS(fromDouble(Math::cos)),
    COSH(fromDouble(Math::cosh)),
    DEGREES(fromDouble(Math::toDegrees)),
    E((Object l) -> Math.E),
    EXP(fromDouble(Math::exp)),
    EXPM1(fromDouble(Math::expm1)),
    FLOOR(fromDouble(Math::floor)),
    LOG(fromDouble(Math::log)),
    LOG10(fromDouble(Math::log10)),
    PI((Object l) -> Math.PI),
    RADIANS(fromDouble(Math::toRadians)),
    ROUND(fromDouble(Math::round)),
    SIN(fromDouble(Math::sin)),
    SINH(fromDouble(Math::sinh)),
    SQRT(fromDouble(Math::sqrt)),
    TAN(fromDouble(Math::tan));

    private final Function<Object, Object> apply;

    MathProcessor(Function<Object, Object> apply) {
        this.apply = apply;
    }

    private static Function<Object, Object> fromDouble(DoubleFunction<Object> apply) {
        return (Object l) -> apply.apply(((Number) l).doubleValue());
    }

    public final Object apply(Object l) {
        return apply.apply(l);
    }
}
