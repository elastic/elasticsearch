/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.expression.gen.pipeline.AttributeInput;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.ConstantInput;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Source;

import java.util.List;
import java.util.Objects;

/**
 * An expression that has a name. Named expressions can be used as a result
 * (by converting to an attribute).
 */
public abstract class NamedExpression extends Expression {

    private final String name;
    private final ExpressionId id;
    private final boolean synthetic;
    private Pipe lazyPipe = null;


    public NamedExpression(Source source, String name, List<Expression> children, ExpressionId id) {
        this(source, name, children, id, false);
    }

    public NamedExpression(Source source, String name, List<Expression> children, ExpressionId id, boolean synthetic) {
        super(source, children);
        this.name = name;
        this.id = id == null ? new ExpressionId() : id;
        this.synthetic = synthetic;
    }

    public String name() {
        return name;
    }

    public ExpressionId id() {
        return id;
    }

    public boolean synthetic() {
        return synthetic;
    }

    public abstract Attribute toAttribute();

    public Pipe asPipe() {
        if (lazyPipe == null) {
            lazyPipe = foldable() ? new ConstantInput(source(), this, fold()) : makePipe();
        }

        return lazyPipe;
    }

    protected Pipe makePipe() {
        return new AttributeInput(source(), this, toAttribute());
    }

    public abstract ScriptTemplate asScript();

    @Override
    public int hashCode() {
        return Objects.hash(id, name, synthetic);
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
                && Objects.equals(id, other.id)
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
}