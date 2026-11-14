/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.unit;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.util.function.Function;

/**
 * An expression that knows the unit of its result from the units of its inputs.
 * <p>
 * An expression that does not implement it has an unknown unit. Implement it only when the rule holds for
 * every input: a missing unit is fine, a wrong one is not.
 */
public interface UnitAware {

    /**
     * @param unitOf the unit of an input expression, or null when unknown
     * @return the unit of this expression's result, or null when unknown
     */
    @Nullable
    Unit unit(Function<Expression, Unit> unitOf);

    /**
     * The result has the unit of one input: {@code SUM}, {@code MAX}, {@code ABS}, casts.
     */
    interface PreservesUnit extends UnitAware {
        /** The input whose unit the result keeps. */
        @Nullable
        Expression unitSource();

        @Override
        default Unit unit(Function<Expression, Unit> unitOf) {
            Expression source = unitSource();
            return source == null ? null : unitOf.apply(source);
        }
    }

    /**
     * The result is a count: {@code COUNT}, {@code COUNT_DISTINCT}.
     */
    interface Unitless extends UnitAware {
        @Override
        default Unit unit(Function<Expression, Unit> unitOf) {
            return Unit.ONE;
        }
    }

    /**
     * The result is the input's unit per second: {@code RATE}, {@code IRATE}, {@code DERIV}.
     */
    interface PerSecond extends UnitAware {
        /** The input whose unit is divided by seconds. */
        @Nullable
        Expression unitSource();

        @Override
        default Unit unit(Function<Expression, Unit> unitOf) {
            Expression source = unitSource();
            Unit unit = source == null ? null : unitOf.apply(source);
            return unit == null ? null : unit.over(Unit.SECOND);
        }
    }
}
