/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;
import org.elasticsearch.xpack.esql.core.util.PlanStreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static org.elasticsearch.TransportVersions.ESQL_QUALIFIERS_IN_ATTRIBUTES;

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

    // can the attr be null
    private final Nullability nullability;
    private final String qualifier;

    public Attribute(Source source, String name, @Nullable NameId id) {
        this(source, name, Nullability.TRUE, id);
    }

    public Attribute(Source source, @Nullable String qualifier, String name, @Nullable NameId id) {
        this(source, qualifier, name, Nullability.TRUE, id);
    }

    public Attribute(Source source, String name, Nullability nullability, @Nullable NameId id) {
        this(source, null, name, nullability, id);
    }

    public Attribute(Source source, @Nullable String qualifier, String name, Nullability nullability, @Nullable NameId id) {
        this(source, qualifier, name, nullability, id, false);
    }

    public Attribute(Source source, String name, Nullability nullability, @Nullable NameId id, boolean synthetic) {
        this(source, null, name, nullability, id, synthetic);
    }

    public Attribute(
        Source source,
        @Nullable String qualifier,
        String name,
        Nullability nullability,
        @Nullable NameId id,
        boolean synthetic
    ) {
        super(source, name, emptyList(), id, synthetic);
        this.qualifier = qualifier;
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

    public String qualifier() {
        return qualifier;
    }

    public String qualifiedName() {
        return qualifier != null ? "[" + qualifier + "].[" + name() + "]" : name();
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
        return Objects.equals(source(), source) ? this : clone(source, qualifier(), name(), dataType(), nullable(), id(), synthetic());
    }

    public Attribute withQualifier(String qualifier) {
        return Objects.equals(qualifier, qualifier) ? this : clone(source(), qualifier, name(), dataType(), nullable(), id(), synthetic());
    }

    public Attribute withName(String name) {
        return Objects.equals(name(), name) ? this : clone(source(), qualifier(), name, dataType(), nullable(), id(), synthetic());
    }

    public Attribute withNullability(Nullability nullability) {
        return Objects.equals(nullable(), nullability)
            ? this
            : clone(source(), qualifier(), name(), dataType(), nullability, id(), synthetic());
    }

    public Attribute withId(NameId id) {
        return clone(source(), qualifier(), name(), dataType(), nullable(), id, synthetic());
    }

    public Attribute withDataType(DataType type) {
        return Objects.equals(dataType(), type) ? this : clone(source(), qualifier(), name(), type, nullable(), id(), synthetic());
    }

    protected abstract Attribute clone(
        Source source,
        String qualifier,
        String name,
        DataType type,
        Nullability nullability,
        NameId id,
        boolean synthetic
    );

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
        return clone(Source.EMPTY, qualifier(), name(), dataType(), nullability, id(), synthetic());
    }

    @Override
    @SuppressWarnings("checkstyle:EqualsHashCode")// equals is implemented in parent. See innerEquals instead
    public int hashCode() {
        return Objects.hash(super.hashCode(), qualifier, nullability);
    }

    @Override
    protected boolean innerEquals(Object o) {
        var other = (Attribute) o;
        return super.innerEquals(other) && Objects.equals(qualifier, other.qualifier) && Objects.equals(nullability, other.nullability);
    }

    @Override
    public String toString() {
        return qualifiedName() + "{" + label() + (synthetic() ? "$" : "") + "}" + "#" + id();
    }

    @Override
    public String nodeString() {
        return toString();
    }

    protected abstract String label();

    /**
     * Compares the size and datatypes of two lists of attributes for equality.
     */
    public static boolean dataTypeEquals(List<Attribute> left, List<Attribute> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int i = 0; i < left.size(); i++) {
            if (left.get(i).dataType() != right.get(i).dataType()) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return true if the attribute represents a TSDB dimension type
     */
    public abstract boolean isDimension();

    protected void checkAndSerializeQualifier(PlanStreamOutput out, TransportVersion version) throws IOException {
        if (version.onOrAfter(ESQL_QUALIFIERS_IN_ATTRIBUTES)) {
            out.writeOptionalCachedString(qualifier());
        } else if (qualifier() != null) {
            // Non-null qualifier means the query specifically defined one. Old nodes don't know what to do with it and just writing
            // null would lose information and lead to undefined, likely invalid queries.
            // IllegalArgumentException returns a 400 to the user, which is what we want here.
            throw new IllegalArgumentException("Trying to serialize an Attribute with a qualifier to an old node");
        }
    }

    protected static String readQualifier(PlanStreamInput in, TransportVersion version) throws IOException {
        if (version.onOrAfter(ESQL_QUALIFIERS_IN_ATTRIBUTES)) {
            return in.readOptionalCachedString();
        }
        return null;
    }
}
