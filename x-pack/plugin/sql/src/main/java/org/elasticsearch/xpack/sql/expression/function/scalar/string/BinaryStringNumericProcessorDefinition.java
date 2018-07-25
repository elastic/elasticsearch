/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.BinaryProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BinaryStringNumericProcessor.BinaryStringNumericOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.Objects;

/**
 * Processor definition for String operations requiring one string and one numeric argument.
 */
public class BinaryStringNumericProcessorDefinition extends BinaryProcessorDefinition {

    private final BinaryStringNumericOperation operation;

    public BinaryStringNumericProcessorDefinition(Location location, Expression expression, ProcessorDefinition left,
            ProcessorDefinition right, BinaryStringNumericOperation operation) {
        super(location, expression, left, right);
        this.operation = operation;
    }

    @Override
    protected NodeInfo<BinaryStringNumericProcessorDefinition> info() {
        return NodeInfo.create(this, BinaryStringNumericProcessorDefinition::new, expression(), left(), right(), operation());
    }

    public BinaryStringNumericOperation operation() {
        return operation;
    }

    @Override
    protected BinaryProcessorDefinition replaceChildren(ProcessorDefinition newLeft, ProcessorDefinition newRight) {
        return new BinaryStringNumericProcessorDefinition(location(), expression(), newLeft, newRight, operation());
    }

    @Override
    public BinaryStringNumericProcessor asProcessor() {
        return new BinaryStringNumericProcessor(left().asProcessor(), right().asProcessor(), operation());
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

        BinaryStringNumericProcessorDefinition other = (BinaryStringNumericProcessorDefinition) obj;
        return Objects.equals(operation, other.operation)
                && Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
    }
}
