/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;
import org.elasticsearch.xpack.sql.tree.Node;

import java.util.List;

public abstract class ProcessorDefinition extends Node<ProcessorDefinition> {

    private final Expression expression;

    public ProcessorDefinition(Expression expression, List<ProcessorDefinition> children) {
        super(children);
        this.expression = expression;
    }

    public Expression expression() {
        return expression;
    }

    public abstract Processor asProcessor();
}
