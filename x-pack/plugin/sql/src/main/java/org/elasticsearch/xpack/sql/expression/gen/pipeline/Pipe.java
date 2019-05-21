/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.pipeline;

import org.elasticsearch.xpack.sql.capabilities.Resolvable;
import org.elasticsearch.xpack.sql.capabilities.Resolvables;
import org.elasticsearch.xpack.sql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.tree.Node;
import org.elasticsearch.xpack.sql.tree.Source;

import java.util.ArrayList;
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
public abstract class Pipe extends Node<Pipe> implements FieldExtraction, Resolvable {

    private final Expression expression;

    public Pipe(Source source, Expression expression, List<Pipe> children) {
        super(source, children);
        this.expression = expression;
    }

    public Expression expression() {
        return expression;
    }

    @Override
    public boolean resolved() {
        return Resolvables.resolved(children());
    }

    @Override
    public void collectFields(SqlSourceBuilder sourceBuilder) {
        children().forEach(c -> c.collectFields(sourceBuilder));
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return children().stream().anyMatch(Pipe::supportedByAggsOnlyQuery);
    }

    public abstract Processor asProcessor();

    /**
     * Resolve {@link Attribute}s which are unprocessable into
     * {@link Pipe}s that are.
     *
     * @return {@code this} if the resolution doesn't change the
     *      definition, a new {@link Pipe} otherwise
     */
    public Pipe resolveAttributes(AttributeResolver resolver) {
        List<Pipe> newPipes = new ArrayList<>(children().size());
        for (Pipe p : children()) {
            newPipes.add(p.resolveAttributes(resolver));
        }

        return children().equals(newPipes) ? this : replaceChildren(newPipes);
    }

    public interface AttributeResolver {
        FieldExtraction resolve(Attribute attribute);
    }
}
