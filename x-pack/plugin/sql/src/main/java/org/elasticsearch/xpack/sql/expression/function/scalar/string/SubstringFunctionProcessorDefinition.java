/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class SubstringFunctionProcessorDefinition extends ProcessorDefinition {

    private final ProcessorDefinition source, start, length;

    public SubstringFunctionProcessorDefinition(Location location, Expression expression, ProcessorDefinition source,
            ProcessorDefinition start, ProcessorDefinition length) {
        super(location, expression, Arrays.asList(source, start, length));
        this.source = source;
        this.start = start;
        this.length = length;
    }

    @Override
    public final ProcessorDefinition replaceChildren(List<ProcessorDefinition> newChildren) {
        if (newChildren.size() != 3) {
            throw new IllegalArgumentException("expected [3] children but received [" + newChildren.size() + "]");
        }
        return replaceChildren(newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    public final ProcessorDefinition resolveAttributes(AttributeResolver resolver) {
        ProcessorDefinition newSource = source.resolveAttributes(resolver);
        ProcessorDefinition newStart = start.resolveAttributes(resolver);
        ProcessorDefinition newLength = length.resolveAttributes(resolver);
        if (newSource == source && newStart == start && newLength == length) {
            return this;
        }
        return replaceChildren(newSource, newStart, newLength);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return source.supportedByAggsOnlyQuery() && start.supportedByAggsOnlyQuery() && length.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return source.resolved() && start.resolved() && length.resolved();
    }

    protected ProcessorDefinition replaceChildren(ProcessorDefinition newSource, ProcessorDefinition newStart,
            ProcessorDefinition newLength) {
        return new SubstringFunctionProcessorDefinition(location(), expression(), newSource, newStart, newLength);
    }

    @Override
    public final void collectFields(SqlSourceBuilder sourceBuilder) {
        source.collectFields(sourceBuilder);
        start.collectFields(sourceBuilder);
        length.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<SubstringFunctionProcessorDefinition> info() {
        return NodeInfo.create(this, SubstringFunctionProcessorDefinition::new, expression(), source, start, length);
    }

    @Override
    public SubstringFunctionProcessor asProcessor() {
        return new SubstringFunctionProcessor(source.asProcessor(), start.asProcessor(), length.asProcessor());
    }
    
    public ProcessorDefinition source() {
        return source;
    }
    
    public ProcessorDefinition start() {
        return start;
    }
    
    public ProcessorDefinition length() {
        return length;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, start, length);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SubstringFunctionProcessorDefinition other = (SubstringFunctionProcessorDefinition) obj;
        return Objects.equals(source, other.source) && Objects.equals(start, other.start) && Objects.equals(length, other.length);
    }
}
