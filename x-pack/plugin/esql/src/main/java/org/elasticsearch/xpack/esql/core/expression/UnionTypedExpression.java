/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.UnionEsField;

import java.util.List;

/**
 * An expression of type {@link DataType#UNION}, together with the leg types that say what its values' tags mean.
 * <p>
 * {@code UNION} is the one type whose name does not determine how to read a value: a value carries a tag, and the tag
 * is an index into a list of types. The list is therefore not metadata about the column, it is half of the column's
 * type, and anything holding the type without it holds something unreadable. {@link UnionEsField} carries it for a
 * field; {@link ReferenceAttribute} carries it across the aliasing boundary, which is what {@code RENAME} and
 * {@code EVAL} make; and {@link Alias} answers for its child. Consumers ask {@link #legTypesOf} rather than testing
 * for a field, so a column that stops being a field does not stop being readable.
 * </p>
 * <p>
 * TODO: this is the same workaround, for the same reason, as {@link AnalyzedTextExpression} - and that interface's own
 * TODO states the root cause. The leg list is a parameter of the type, so if {@link DataType} could carry parameters
 * ({@code union(long, keyword)}, which is what ClickHouse's {@code Variant(UInt64, String)} is) it would flow with the
 * type through every site that mints and serializes an attribute, with no per-site propagation and no capability
 * interface - and a legless union would be unrepresentable rather than merely refused at construction. {@code DataType}
 * is a Java enum compared by identity in equality, in plan verification and in every switch, so parameterising it is
 * not a change this proof of concept can make. Two capability interfaces for the same missing feature is the signal
 * that the enum is the thing to revisit.
 * </p>
 */
public interface UnionTypedExpression {

    /** The types this expression's values can have, in the canonical leg order a tag indexes. */
    List<DataType> unionLegTypes();

    /**
     * The leg types of {@code expression}, or {@code null} when it is not union-typed. An {@link Alias} answers for its
     * child, because an alias is the same column under another name and {@link Alias#toAttribute} is what carries the
     * legs onto the reference it mints.
     */
    @Nullable
    static List<DataType> legTypesOf(Expression expression) {
        if (expression instanceof Alias alias) {
            return legTypesOf(alias.child());
        }
        return expression instanceof UnionTypedExpression union ? union.unionLegTypes() : null;
    }
}
