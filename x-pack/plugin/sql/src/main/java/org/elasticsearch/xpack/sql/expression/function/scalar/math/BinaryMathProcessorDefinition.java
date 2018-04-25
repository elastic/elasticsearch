/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryMathProcessor.BinaryMathOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.BinaryProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.Objects;

/**
 * Processor definition for math operations requiring two arguments.
 */
public class BinaryMathProcessorDefinition extends BinaryProcessorDefinition {

    private final BinaryMathOperation operation;

    public BinaryMathProcessorDefinition(Location location, Expression expression, ProcessorDefinition left,
            ProcessorDefinition right, BinaryMathOperation operation) {
        super(location, expression, left, right);
        this.operation = operation;
    }

    @Override
    protected NodeInfo<BinaryMathProcessorDefinition> info() {
        return NodeInfo.create(this, BinaryMathProcessorDefinition::new, expression(), left(), right(), operation);
    }

    public BinaryMathOperation operation() {
        return operation;
    }

    @Override
    protected BinaryProcessorDefinition replaceChildren(ProcessorDefinition left, ProcessorDefinition right) {
        return new BinaryMathProcessorDefinition(location(), expression(), left, right, operation);
    }

    @Override
    public BinaryMathProcessor asProcessor() {
        return new BinaryMathProcessor(left().asProcessor(), right().asProcessor(), operation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left(), right(), operation);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        BinaryMathProcessorDefinition other = (BinaryMathProcessorDefinition) obj;
        return Objects.equals(operation, other.operation)
                && Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
    }
}
