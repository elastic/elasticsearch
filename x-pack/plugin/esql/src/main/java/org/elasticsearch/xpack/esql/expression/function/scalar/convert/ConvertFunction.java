/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.UnionTypeEsField;

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

    /**
     * Whether this conversion can be one of the per-source-type conversions of a {@link UnionTypeEsField}. Those run in
     * {@code ValuesSourceReaderOperator} through {@code TypeConverter}, which binds every input of the conversion to the loaded
     * block, so the field must be the only input the conversion evaluates. By default, when the field is the only child.
     * <p>
     * This default is the safe one: a new argument someone forgets to account for makes the conversion fail on union types,
     * instead of silently reading the field's values as that argument.
     */
    default boolean canBeUnionTypeEsFieldConversion() {
        return ((Expression) this).children().size() == 1;
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
