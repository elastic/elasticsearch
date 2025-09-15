/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

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
    /**
     * Changing this will break bwc with 8.15, see {@link FieldAttribute#fieldName()}.
     */
    protected static final String SYNTHETIC_ATTRIBUTE_NAME_PREFIX = "$$";

    // can the attr be null - typically used in JOINs
    private final Nullability nullability;

    public Attribute(Source source, String name, @Nullable NameId id) {
        this(source, name, Nullability.TRUE, id);
    }

    public Attribute(Source source, String name, Nullability nullability, @Nullable NameId id) {
        this(source, name, nullability, id, false);
    }

    public Attribute(Source source, String name, Nullability nullability, @Nullable NameId id, boolean synthetic) {
        super(source, name, emptyList(), id, synthetic);
        this.nullability = nullability;
    }

    public static String rawTemporaryName(String... parts) {
        var name = String.join("$", parts);
        return name.isEmpty() || name.startsWith(SYNTHETIC_ATTRIBUTE_NAME_PREFIX) ? name : SYNTHETIC_ATTRIBUTE_NAME_PREFIX + name;
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
    }

    @Override
    public Nullability nullable() {
        return nullability;
    }

    @Override
    public AttributeSet references() {
        return AttributeSet.of(this);
    }

    public Attribute withLocation(Source source) {
        return Objects.equals(source(), source) ? this : clone(source, name(), dataType(), nullable(), id(), synthetic());
    }

    public Attribute withName(String name) {
        return Objects.equals(name(), name) ? this : clone(source(), name, dataType(), nullable(), id(), synthetic());
    }

    public Attribute withNullability(Nullability nullability) {
        return Objects.equals(nullable(), nullability) ? this : clone(source(), name(), dataType(), nullability, id(), synthetic());
    }

    public Attribute withId(NameId id) {
        return clone(source(), name(), dataType(), nullable(), id, synthetic());
    }

    public Attribute withDataType(DataType type) {
        return Objects.equals(dataType(), type) ? this : clone(source(), name(), type, nullable(), id(), synthetic());
    }

    protected abstract Attribute clone(Source source, String name, DataType type, Nullability nullability, NameId id, boolean synthetic);

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
        return clone(Source.EMPTY, name(), dataType(), nullability, id(), synthetic());
    }

    @Override
    @SuppressWarnings("checkstyle:EqualsHashCode")// equals is implemented in parent. See innerEquals instead
    public int hashCode() {
        return Objects.hash(super.hashCode(), nullability);
    }

    @Override
    protected boolean innerEquals(Object o) {
        var other = (Attribute) o;
        return super.innerEquals(other) && Objects.equals(nullability, other.nullability);
    }

    @Override
    public String toString() {
        return name() + "{" + label() + (synthetic() ? "$" : "") + "}" + "#" + id();
    }

    @Override
    public String nodeString() {
        return toString();
    }

    protected abstract String label();

    /**
     * If this field is unsupported this contains the underlying ES types. If there
     * is a type conflict this will have many elements, some or all of which may
     * be actually supported types.
     */
    @Nullable
    public List<String> originalTypes() {
        return null;
    }
}
