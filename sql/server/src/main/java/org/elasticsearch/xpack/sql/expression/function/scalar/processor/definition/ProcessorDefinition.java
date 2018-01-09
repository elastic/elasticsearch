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
import org.elasticsearch.xpack.sql.querydsl.container.ColumnReference;
import org.elasticsearch.xpack.sql.tree.Node;

import java.util.List;

public abstract class ProcessorDefinition extends Node<ProcessorDefinition> implements FieldExtraction {

    private final Expression expression;

    public ProcessorDefinition(Expression expression, List<ProcessorDefinition> children) {
        super(children);
        this.expression = expression;
    }

    public Expression expression() {
        return expression;
    }

    public abstract boolean resolved();

    public abstract Processor asProcessor();

    /**
     * Resolve {@link Attribute}s which are unprocessable into
     * {@link ColumnReference}s which are processable.
     *
     * @return {@code this} if the resolution doesn't change the
     *      definition, a new {@link ProcessorDefinition} otherwise
     */
    public abstract ProcessorDefinition resolveAttributes(AttributeResolver resolver);
    public interface AttributeResolver {
        ColumnReference resolve(Attribute attribute);
    }
}
