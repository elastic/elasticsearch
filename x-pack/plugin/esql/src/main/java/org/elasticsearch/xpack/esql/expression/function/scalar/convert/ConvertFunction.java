/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Set;

/**
 * A function that converts from one type to another.
 */
public interface ConvertFunction {
    /**
     * Whether a cast, or a type conversion function, resolves for every input type it can't convert, and then evaluates to
     * {@code null}. Every node taking part in the query must support it: older data nodes fail when asked to evaluate a conversion
     * of an input type it doesn't have an evaluator for, so below this version the {@code Verifier} fails such a conversion with
     * the error it always had.
     */
    TransportVersion LENIENT_CAST = TransportVersion.fromName("esql_lenient_cast");

    /**
     * Expression containing the values to be converted.
     */
    Expression field();

    /**
     * The types that {@link #field()} can have.
     */
    Set<DataType> supportedTypes();

    /**
     * Whether {@link #field()} has a type this function converts, or is {@code null}. A conversion that resolved without it
     * (see {@link #lenientOnInputType()}) evaluates to {@code null}.
     * <p>
     * Don't use {@code resolved()} for this: the per-type conversions of a union field are built with the unwidened type of each
     * mapped type ({@code short}, {@code float}) and aren't resolved, but they do convert.
     * </p>
     */
    default boolean convertsInput() {
        DataType type = field().dataType();
        return type == DataType.NULL || supportedTypes().contains(type.widenSmallNumeric());
    }

    /**
     * Whether this function resolves, and evaluates to {@code null}, when its input has a type it can't convert, instead of failing.
     * Only the type of the input is lenient: every other argument, like the options of {@code TO_IP}, is checked as before.
     * <p>
     * It is {@code false} for functions that can't give a {@code null} of the right type, or that need more than a value:
     * <ul>
     *     <li>{@code TO_GAUGE} and {@code TO_COUNTER}: the type of their result follows the type of their input.</li>
     *     <li>{@code TO_DATEPERIOD} and {@code TO_TIMEDURATION}: they need a constant.</li>
     *     <li>{@code TO_LONG(x, base)} and {@code TO_INTEGER(x, base)}: they only parse strings.</li>
     *     <li>{@code FromAggregateMetricDouble}: internal.</li>
     * </ul>
     */
    default boolean lenientOnInputType() {
        return true;
    }

    /**
     * The type resolution this function has when its input type isn't lenient. A conversion that resolved leniently but whose
     * input it doesn't convert fails with this when some node can't take part in a lenient cast.
     */
    Expression.TypeResolution strictTypeResolution();

    /**
     * The type this function converts to.
     */
    DataType dataType();

    /**
     * Whether converting a value of {@code field().dataType()} using this function leaves it unchanged, so the cast can be dropped. By
     * default, this holds when the {@code field().dataType()} already has the output type. Widening conversions (e.g., {@code half_float}
     * to {@code double}) are not no-ops even though they share a representation, since the declared type changes.
     */
    default boolean isNoop() {
        return field().dataType() == dataType();
    }

    /**
     * An explicit {@code NULL} literal, or a cast / conversion of one ({@code null::string}, {@code TO_INTEGER(NULL)}).
     * A typed null is not itself a misuse; callers that consume it still are.
     */
    static boolean isExplicitNull(Expression e) {
        if (e instanceof Literal literal && literal.value() == null && literal.sourceText().equalsIgnoreCase("null")) {
            return true;
        }
        return e instanceof ConvertFunction convert && isExplicitNull(convert.field());
    }
}
