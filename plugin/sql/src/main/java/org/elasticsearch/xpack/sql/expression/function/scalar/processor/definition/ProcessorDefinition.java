/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.xpack.sql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.Node;

import java.util.List;

/**
 * Contains the tree for processing a function, so for example, the {@code ProcessorDefinition} of:
 *
 * ABS(MAX(foo)) + CAST(bar)
 *
 * Is an {@code Add} Function with left {@code ABS} over an aggregate (MAX), and
 * right being a {@code CAST} function.
 */
public abstract class ProcessorDefinition extends Node<ProcessorDefinition> implements FieldExtraction {

    private final Expression expression;

    public ProcessorDefinition(Location location, Expression expression, List<ProcessorDefinition> children) {
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
     * {@link FieldExtraction}s which are processable.
     *
     * @return {@code this} if the resolution doesn't change the
     *      definition, a new {@link ProcessorDefinition} otherwise
     */
    public abstract ProcessorDefinition resolveAttributes(AttributeResolver resolver);
    public interface AttributeResolver {
        FieldExtraction resolve(Attribute attribute);
    }
}
