/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Objects;

/**
 * SQL Literal or constant.
 */
public class Literal extends LeafExpression {

    public static final Literal TRUE = new Literal(Source.EMPTY, Boolean.TRUE, DataType.BOOLEAN);
    public static final Literal FALSE = new Literal(Source.EMPTY, Boolean.FALSE, DataType.BOOLEAN);
    public static final Literal NULL = new Literal(Source.EMPTY, null, DataType.NULL);

    private final Object value;
    private final DataType dataType;

    public Literal(Source source, Object value, DataType dataType) {
        super(source);
        this.dataType = dataType;
        this.value = value;
    }

    @Override
    protected NodeInfo<? extends Literal> info() {
        return NodeInfo.create(this, Literal::new, value, dataType);
    }

    public Object value() {
        return value;
    }

    @Override
    public boolean foldable() {
        return true;
    }

    @Override
    public Nullability nullable() {
        return value == null ? Nullability.TRUE : Nullability.FALSE;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }

    @Override
    public boolean resolved() {
        return true;
    }

    @Override
    public Object fold() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, dataType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Literal other = (Literal) obj;
        return Objects.equals(value, other.value) && Objects.equals(dataType, other.dataType);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }

    @Override
    public String nodeString() {
        return toString() + "[" + dataType + "]";
    }

    /**
     * Utility method for creating 'in-line' Literals (out of values instead of expressions).
     */
    public static Literal of(Source source, Object value) {
        if (value instanceof Literal) {
            return (Literal) value;
        }
        return new Literal(source, value, DataTypes.fromJava(value));
    }

    /**
     * Utility method for creating a literal out of a foldable expression.
     * Throws an exception if the expression is not foldable.
     */
    public static Literal of(Expression foldable) {
        if (!foldable.foldable()) {
            throw new QlIllegalArgumentException("Foldable expression required for Literal creation; received unfoldable " + foldable);
        }

        if (foldable instanceof Literal) {
            return (Literal) foldable;
        }

        return new Literal(foldable.source(), foldable.fold(), foldable.dataType());
    }

    public static Literal of(Expression source, Object value) {
        return new Literal(source.source(), value, source.dataType());
    }
}
