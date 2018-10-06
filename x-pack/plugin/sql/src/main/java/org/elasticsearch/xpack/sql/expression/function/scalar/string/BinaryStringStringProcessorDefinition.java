/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.BinaryProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BinaryStringStringProcessor.BinaryStringStringOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.Objects;

/**
 * Processor definition for String operations requiring two string arguments.
 */
public class BinaryStringStringProcessorDefinition extends BinaryProcessorDefinition {

    private final BinaryStringStringOperation operation;

    public BinaryStringStringProcessorDefinition(Location location, Expression expression, ProcessorDefinition left,
            ProcessorDefinition right, BinaryStringStringOperation operation) {
        super(location, expression, left, right);
        this.operation = operation;
    }

    @Override
    protected NodeInfo<BinaryStringStringProcessorDefinition> info() {
        return NodeInfo.create(this, BinaryStringStringProcessorDefinition::new, expression(), left(), right(), operation);
    }

    public BinaryStringStringOperation operation() {
        return operation;
    }

    @Override
    protected BinaryProcessorDefinition replaceChildren(ProcessorDefinition left, ProcessorDefinition right) {
        return new BinaryStringStringProcessorDefinition(location(), expression(), left, right, operation);
    }

    @Override
    public BinaryStringStringProcessor asProcessor() {
        return new BinaryStringStringProcessor(left().asProcessor(), right().asProcessor(), operation);
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

        BinaryStringStringProcessorDefinition other = (BinaryStringStringProcessorDefinition) obj;
        return Objects.equals(operation, other.operation)
                && Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
    }
}
