/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.gen.pipeline;

import org.elasticsearch.xpack.ql.capabilities.Resolvable;
import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.Node;
import org.elasticsearch.xpack.ql.tree.Source;

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
    public void collectFields(QlSourceBuilder sourceBuilder) {
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
