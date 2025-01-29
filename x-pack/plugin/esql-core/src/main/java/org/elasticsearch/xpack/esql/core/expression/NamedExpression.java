/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.Objects;

/**
 * An expression that has a name. Named expressions can be used as a result
 * (by converting to an attribute).
 */
public abstract class NamedExpression extends Expression implements NamedWriteable {

    private final String name;
    private final NameId id;
    private final boolean synthetic;

    public NamedExpression(Source source, String name, List<Expression> children, @Nullable NameId id) {
        this(source, name, children, id, false);
    }

    public NamedExpression(Source source, String name, List<Expression> children, @Nullable NameId id, boolean synthetic) {
        super(source, children);
        this.name = name;
        this.id = id == null ? new NameId() : id;
        this.synthetic = synthetic;
    }

    public String name() {
        return name;
    }

    public NameId id() {
        return id;
    }

    /**
     * Synthetic named expressions are not user defined and usually created during optimizations and substitutions, e.g. when turning
     * {@code ... | STATS x = avg(2*field)} into {@code ... | EVAL $$synth$attribute = 2*field | STATS x = avg($$synth$attribute)}.
     */
    public boolean synthetic() {
        return synthetic;
    }

    /**
     * Try to return either {@code this} if it is an {@link Attribute}, or a {@link ReferenceAttribute} to it otherwise.
     * Return an {@link UnresolvedAttribute} if this is unresolved.
     */
    public abstract Attribute toAttribute();

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), name, synthetic);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        NamedExpression other = (NamedExpression) obj;
        return Objects.equals(synthetic, other.synthetic)
            /*
             * It is important that the line below be `name`
             * and not `name()` because subclasses might override
             * `name()` in ways that are not compatible with
             * equality. Specifically the `Unresolved` subclasses.
             */
            && Objects.equals(name, other.name)
            && Objects.equals(children(), other.children());
    }

    @Override
    public String toString() {
        return super.toString() + "#" + id();
    }

    @Override
    public String nodeString() {
        return name();
    }
}
