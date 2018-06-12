/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.tree.Location;

import java.util.Objects;

import static java.util.Collections.emptyList;

import java.util.List;

/**
 * {@link Expression}s that can be converted into Elasticsearch
 * sorts, aggregations, or queries. They can also be extracted
 * from the result of a search.
 *
 * In the statement {@code SELECT ABS(foo), A, B+C FROM ...} the three named
 * expressions (ABS(foo), A, B+C) get converted to attributes and the user can
 * only see Attributes.
 *
 * In the statement {@code SELECT foo FROM TABLE WHERE foo > 10 + 1} 10+1 is an
 * expression. It's not named - meaning there's no alias for it (defined by the
 * user) and as such there's no attribute - no column to be returned to the user.
 * It's an expression used for filtering so it doesn't appear in the result set
 * (derived table). "foo" on the other hand is an expression, a named expression
 * (it has a name) and also an attribute - it's a column in the result set.
 *
 * Another example {@code SELECT foo FROM ... WHERE bar > 10 +1} "foo" gets
 * converted into an Attribute, bar does not. That's because bar is used for
 * filtering alone but it's not part of the projection meaning the user doesn't
 * need it in the derived table.
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

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
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
        return Objects.equals(location(), location) ? this : clone(location, name(), qualifier(), nullable(), id(), synthetic());
    }

    public Attribute withQualifier(String qualifier) {
        return Objects.equals(qualifier(), qualifier) ? this : clone(location(), name(), qualifier, nullable(), id(), synthetic());
    }

    public Attribute withNullability(boolean nullable) {
        return Objects.equals(nullable(), nullable) ? this : clone(location(), name(), qualifier(), nullable, id(), synthetic());
    }

    protected abstract Attribute clone(Location location, String name, String qualifier, boolean nullable, ExpressionId id,
                                       boolean synthetic);

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
