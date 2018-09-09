/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.BinaryProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.Objects;

public class ConcatFunctionProcessorDefinition extends BinaryProcessorDefinition {

    public ConcatFunctionProcessorDefinition(Location location, Expression expression, ProcessorDefinition left,
            ProcessorDefinition right) {
        super(location, expression, left, right);
    }

    @Override
    protected NodeInfo<ConcatFunctionProcessorDefinition> info() {
        return NodeInfo.create(this, ConcatFunctionProcessorDefinition::new, expression(), left(), right());
    }

    @Override
    protected BinaryProcessorDefinition replaceChildren(ProcessorDefinition left, ProcessorDefinition right) {
        return new ConcatFunctionProcessorDefinition(location(), expression(), left, right);
    }

    @Override
    public ConcatFunctionProcessor asProcessor() {
        return new ConcatFunctionProcessor(left().asProcessor(), right().asProcessor());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(left(), right());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ConcatFunctionProcessorDefinition other = (ConcatFunctionProcessorDefinition) obj;
        return Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
    }
}
