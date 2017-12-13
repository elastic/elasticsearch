/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.ChainingProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;

import java.util.Objects;

import static java.util.Collections.singletonList;

public class UnaryProcessorDefinition extends ProcessorDefinition {

    private final ProcessorDefinition child;
    private final Processor action;

    public UnaryProcessorDefinition(Expression expression, ProcessorDefinition child, Processor action) {
        super(expression, singletonList(child));
        this.child = child;
        this.action = action;
    }

    public ProcessorDefinition child() {
        return child;
    }

    public Processor action() {
        return action;
    }

    @Override
    public boolean resolved() {
        return child.resolved();
    }

    @Override
    public Processor asProcessor() {
        return new ChainingProcessor(child.asProcessor(), action);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression(), child, action);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        UnaryProcessorDefinition other = (UnaryProcessorDefinition) obj;
        return Objects.equals(action, other.action)
                && Objects.equals(child, other.child)
                && Objects.equals(expression(), other.expression());
    }
}
