/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.xpack.sql.expression.Expression;

import java.util.Arrays;

public abstract class BinaryProcessorDefinition extends ProcessorDefinition {

    private final ProcessorDefinition left, right;

    public BinaryProcessorDefinition(Expression expression, ProcessorDefinition left, ProcessorDefinition right) {
        super(expression, Arrays.asList(left, right));
        this.left = left;
        this.right = right;
    }

    public ProcessorDefinition left() {
        return left;
    }

    public ProcessorDefinition right() {
        return right;
    }

    @Override
    public boolean resolved() {
        return left().resolved() && right().resolved();
    }
}
