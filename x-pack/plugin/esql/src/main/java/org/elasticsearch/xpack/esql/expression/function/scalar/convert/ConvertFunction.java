/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Set;

/**
 * A function that converts from one type to another.
 */
public interface ConvertFunction {
    /**
     * Expression containing the values to be converted.
     */
    Expression field();

    /**
     * The types that {@link #field()} can have.
     */
    Set<DataType> supportedTypes();

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
}
