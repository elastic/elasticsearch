/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.gen.script.Params;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypeConversion;
import org.elasticsearch.xpack.sql.type.DataTypes;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

/**
 * SQL Literal or constant.
 */
public class Literal extends NamedExpression {

    public static final Literal TRUE = Literal.of(Source.EMPTY, Boolean.TRUE);
    public static final Literal FALSE = Literal.of(Source.EMPTY, Boolean.FALSE);
    public static final Literal NULL = Literal.of(Source.EMPTY, null);

    private final Object value;
    private final DataType dataType;

    public Literal(Source source, Object value, DataType dataType) {
        this(source, null, value, dataType);
    }

    public Literal(Source source, String name, Object value, DataType dataType) {
        super(source, name == null ? source.text() : name, emptyList(), null);
        this.dataType = dataType;
        this.value = DataTypeConversion.convert(value, dataType);
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
    public Attribute toAttribute() {
        return new LiteralAttribute(source(), name(), null, Nullability.FALSE, id(), false, dataType, this);
    }

    @Override
    public ScriptTemplate asScript() {
        return new ScriptTemplate(String.valueOf(value), Params.EMPTY, dataType);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
    }

    @Override
    public AttributeSet references() {
        return AttributeSet.EMPTY;
    }

    @Override
    protected Expression canonicalize() {
        String s = String.valueOf(value);
        return name().equals(s) ? this : Literal.of(source(), value);
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
        return Objects.equals(value, other.value)
                && Objects.equals(dataType, other.dataType);
    }

    @Override
    public String toString() {
        String s = String.valueOf(value);
        return name().equals(s) ? s : name() + "=" + value;
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
        return of((String) null, foldable);
    }

    public static Literal of(String name, Expression foldable) {
        if (!foldable.foldable()) {
            throw new SqlIllegalArgumentException("Foldable expression required for Literal creation; received unfoldable " + foldable);
        }

        if (foldable instanceof Literal) {
            Literal l = (Literal) foldable;
            if (name == null || l.name().equals(name)) {
                return l;
            }
        }

        Object fold = foldable.fold();

        if (name == null) {
            name = foldable instanceof NamedExpression ? ((NamedExpression) foldable).name() : String.valueOf(fold);
        }
        return new Literal(foldable.source(), name, fold, foldable.dataType());
    }

    public static Literal of(Expression source, Object value) {
        String name = source instanceof NamedExpression ? ((NamedExpression) source).name() : String.valueOf(value);
        return new Literal(source.source(), name, value, source.dataType());
    }
}
