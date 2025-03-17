/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * An {@code Alias} is a {@code NamedExpression} that gets renamed to something else through the Alias.
 *
 * For example, in the statement {@code 5 + 2 AS x}, {@code x} is an alias which is points to {@code ADD(5, 2)}.
 *
 * And in {@code SELECT col AS x} "col" is a named expression that gets renamed to "x" through an alias.
 *
 */
public final class Alias extends NamedExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        NamedExpression.class,
        "Alias",
        Alias::readFrom
    );

    private final Expression child;

    /**
     * Postpone attribute creation until it is actually created.
     * Being immutable, create only one instance.
     */
    private Attribute lazyAttribute;

    /**
     * Sets the qualifier to {@code null}.
     * Mostly for tests and convenience wherever qualifiers aren't required.
     */
    public Alias(Source source, String name, Expression child) {
        this(source, null, name, child, null);
    }

    public Alias(Source source, @Nullable String qualifier, String name, Expression child) {
        this(source, qualifier, name, child, null);
    }

    public Alias(Source source, @Nullable String qualifier, String name, Expression child, @Nullable NameId id) {
        this(source, qualifier, name, child, id, false);
    }

    public Alias(Source source, @Nullable String qualifier, String name, Expression child, @Nullable NameId id, boolean synthetic) {
        super(source, qualifier, name, singletonList(child), id, synthetic);
        this.child = child;
    }

    public static Alias readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((StreamInput & PlanStreamInput) in);
        String name = in.readString();
        String qualifier = in.readOptionalString();
        Expression child = in.readNamedWriteable(Expression.class);
        NameId id = NameId.readFrom((StreamInput & PlanStreamInput) in);
        boolean synthetic = in.readBoolean();

        return new Alias(source, qualifier, name, child, id, synthetic);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeString(name());
        // We used to write the qualifier here. We can still do if needed in the future.
        out.writeOptionalString(qualifier());
        out.writeNamedWriteable(child());
        id().writeTo(out);
        out.writeBoolean(synthetic());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Alias> info() {
        return NodeInfo.create(this, Alias::new, qualifier(), name(), child, id(), synthetic());
    }

    public Alias replaceChild(Expression child) {
        return new Alias(source(), qualifier(), name(), child, id(), synthetic());
    }

    @Override
    public Alias replaceChildren(List<Expression> newChildren) {
        return new Alias(source(), qualifier(), name(), newChildren.get(0), id(), synthetic());
    }

    public Expression child() {
        return child;
    }

    @Override
    public Nullability nullable() {
        return child.nullable();
    }

    @Override
    protected TypeResolution resolveType() {
        return child.resolveType();
    }

    @Override
    public DataType dataType() {
        return child.dataType();
    }

    @Override
    public Attribute toAttribute() {
        if (lazyAttribute == null) {
            lazyAttribute = resolved()
                ? new ReferenceAttribute(source(), qualifier(), name(), dataType(), nullable(), id(), synthetic())
                : new UnresolvedAttribute(source(), qualifier(), name());
        }
        return lazyAttribute;
    }

    @Override
    public String toString() {
        return child + " AS " + qualifiedName() + "#" + id();
    }

    @Override
    public String nodeString() {
        return child.nodeString() + " AS " + qualifiedName();
    }

    /**
     * If the given expression is an alias, return its child - otherwise return as is.
     */
    public static Expression unwrap(Expression e) {
        return e instanceof Alias as ? as.child() : e;
    }
}
