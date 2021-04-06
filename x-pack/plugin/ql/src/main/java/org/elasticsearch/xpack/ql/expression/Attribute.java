/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression;

import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

/**
 * {@link Expression}s that can be materialized and describe properties of the derived table.
 * In other words, an attribute represent a column in the results of a query.
 *
 * In the statement {@code SELECT ABS(foo), A, B+C FROM ...} the three named
 * expressions {@code ABS(foo), A, B+C} get converted to attributes and the user can
 * only see Attributes.
 *
 * In the statement {@code SELECT foo FROM TABLE WHERE foo > 10 + 1} only {@code foo} inside the SELECT
 * is a named expression (an {@code Alias} will be created automatically for it).
 * The rest are not as they are not part of the projection and thus are not part of the derived table.
 */
public abstract class Attribute extends NamedExpression {

    // empty - such as a top level attribute in SELECT cause
    // present - table name or a table name alias
    private final String qualifier;

    // can the attr be null - typically used in JOINs
    private final Nullability nullability;

    public Attribute(Source source, String name, String qualifier, NameId id) {
        this(source, name, qualifier, Nullability.TRUE, id);
    }

    public Attribute(Source source, String name, String qualifier, Nullability nullability, NameId id) {
        this(source, name, qualifier, nullability, id, false);
    }

    public Attribute(Source source, String name, String qualifier, Nullability nullability, NameId id, boolean synthetic) {
        super(source, name, emptyList(), id, synthetic);
        this.qualifier = qualifier;
        this.nullability = nullability;
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
    public Nullability nullable() {
        return nullability;
    }

    @Override
    public AttributeSet references() {
        return new AttributeSet(this);
    }

    public Attribute withLocation(Source source) {
        return Objects.equals(source(), source) ? this : clone(source, name(), dataType(), qualifier(), nullable(), id(), synthetic());
    }

    public Attribute withQualifier(String qualifier) {
        return Objects.equals(qualifier(), qualifier) ? this : clone(source(), name(), dataType(), qualifier, nullable(), id(),
                synthetic());
    }

    public Attribute withName(String name) {
        return Objects.equals(name(), name) ? this : clone(source(), name, dataType(), qualifier(), nullable(), id(), synthetic());
    }

    public Attribute withNullability(Nullability nullability) {
        return Objects.equals(nullable(), nullability) ? this : clone(source(), name(), dataType(), qualifier(), nullability, id(),
                synthetic());
    }

    public Attribute withId(NameId id) {
        return clone(source(), name(), dataType(), qualifier(), nullable(), id, synthetic());
    }

    public Attribute withDataType(DataType type) {
        return Objects.equals(dataType(), type) ? this : clone(source(), name(), type, qualifier(), nullable(), id(), synthetic());
    }

    protected abstract Attribute clone(Source source, String name, DataType type, String qualifier, Nullability nullability,
            NameId id, boolean synthetic);

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
    protected Expression canonicalize() {
        return clone(Source.EMPTY, name(), dataType(), qualifier, nullability, id(), synthetic());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), qualifier, nullability);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            Attribute other = (Attribute) obj;
            return Objects.equals(qualifier, other.qualifier)
                    && Objects.equals(nullability, other.nullability);
        }

        return false;
    }

    @Override
    public String toString() {
        return qualifiedName() + "{" + label() + "}" + "#" + id();
    }

    @Override
    public String nodeString() {
        return toString();
    }

    protected abstract String label();
}
