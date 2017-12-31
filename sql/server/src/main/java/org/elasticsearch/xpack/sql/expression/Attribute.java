/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Objects;

import static java.util.Collections.emptyList;

/**
 * {@link Expression}s that can be converted into Elasticsearch
 * sorts, aggregations, or queries. They can also be extracted
 * from the result of a search.
 */
public abstract class Attribute extends NamedExpression {

    // empty - such as a top level attribute in SELECT cause
    // present - table name or a table name alias
    private final String qualifier;

    // can the attr be null - typically used in JOINs
    private final boolean nullable;

    public Attribute(Location location, String name, String qualifier, ExpressionId id) {
        this(location, name, qualifier, true, id);
    }

    public Attribute(Location location, String name, String qualifier, boolean nullable, ExpressionId id) {
        this(location, name, qualifier, nullable, id, false);
    }

    public Attribute(Location location, String name, String qualifier, boolean nullable, ExpressionId id, boolean synthetic) {
        super(location, name, emptyList(), id, synthetic);
        this.qualifier = qualifier;
        this.nullable = nullable;
    }

    public String qualifier() {
        return qualifier;
    }

    public String qualifiedName() {
        return qualifier == null ? name() : qualifier + "." + name();
    }

    @Override
    public boolean nullable() {
        return nullable;
    }

    @Override
    public AttributeSet references() {
        return new AttributeSet(this);
    }

    public Attribute withLocation(Location location) {
        return Objects.equals(location(), location) ? this : clone(location, name(), dataType(), qualifier(),
                nullable(), id(), synthetic());
    }

    public Attribute withQualifier(String qualifier) {
        return Objects.equals(qualifier(), qualifier) ? this : clone(location(), name(), dataType(), qualifier,
                nullable(), id(), synthetic());
    }

    public Attribute withName(String name) {
        return Objects.equals(name(), name) ? this : clone(location(), name, dataType(), qualifier(), nullable(),
                id(), synthetic());
    }

    public Attribute withNullability(boolean nullable) {
        return Objects.equals(nullable(), nullable) ? this : clone(location(), name(), dataType(), qualifier(),
                nullable, id(), synthetic());
    }

    public Attribute withId(ExpressionId id) {
        return Objects.equals(id(), id) ? this : clone(location(), name(), dataType(), qualifier(), nullable(),
                id, synthetic());
    }

    protected abstract Attribute clone(Location location, String name, DataType dataType, String qualifier,
            boolean nullable, ExpressionId id, boolean synthetic);

    @Override
    public Attribute toAttribute() {
        return this;
    }

    @Override
    public int semanticHash() {
        return id().hashCode();
    }

    @Override
    public boolean semanticEquals(Expression other) {
        return other instanceof Attribute ? id().equals(((Attribute) other).id()) : false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), qualifier, nullable);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            Attribute other = (Attribute) obj;
            return Objects.equals(qualifier, other.qualifier)
                    && Objects.equals(nullable, other.nullable);
        }

        return false;
    }

    @Override
    public String toString() {
        return name() + "{" + label() + "}" + "#" + id();
    }

    protected abstract String label();
}
