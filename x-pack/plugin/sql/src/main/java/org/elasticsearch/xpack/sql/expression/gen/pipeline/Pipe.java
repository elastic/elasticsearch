/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.pipeline;

import org.elasticsearch.xpack.sql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.Node;

import java.util.List;

/**
 * Processing pipe for an expression (tree). Used for local execution of expressions
 * on the invoking node.
 * For example, the {@code Pipe} of:
 *
 * ABS(MAX(foo)) + CAST(bar)
 *
 * Is an {@code Add} operator with left {@code ABS} over an aggregate (MAX), and
 * right being a {@code CAST} function.
 */
public abstract class Pipe extends Node<Pipe> implements FieldExtraction {

    private final Expression expression;

    public Pipe(Location location, Expression expression, List<Pipe> children) {
        super(location, children);
        this.expression = expression;
    }

    public Expression expression() {
        return expression;
    }

    public abstract boolean resolved();

    public abstract Processor asProcessor();

    /**
     * Resolve {@link Attribute}s which are unprocessable into
     * {@link Pipe}s that are.
     *
     * @return {@code this} if the resolution doesn't change the
     *      definition, a new {@link Pipe} otherwise
     */
    public abstract Pipe resolveAttributes(AttributeResolver resolver);

    public interface AttributeResolver {
        FieldExtraction resolve(Attribute attribute);
    }
}
