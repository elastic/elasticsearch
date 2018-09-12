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

public class InsertFunctionProcessorDefinition extends ProcessorDefinition {

    private final ProcessorDefinition source, start, length, replacement;

    public InsertFunctionProcessorDefinition(Location location, Expression expression, 
            ProcessorDefinition source, ProcessorDefinition start, 
            ProcessorDefinition length, ProcessorDefinition replacement) {
        super(location, expression, Arrays.asList(source, start, length, replacement));
        this.source = source;
        this.start = start;
        this.length = length;
        this.replacement = replacement;
    }

    @Override
    public final ProcessorDefinition replaceChildren(List<ProcessorDefinition> newChildren) {
        if (newChildren.size() != 4) {
            throw new IllegalArgumentException("expected [4] children but received [" + newChildren.size() + "]");
        }
        return replaceChildren(newChildren.get(0), newChildren.get(1), newChildren.get(2), newChildren.get(3));
    }
    
    @Override
    public final ProcessorDefinition resolveAttributes(AttributeResolver resolver) {
        ProcessorDefinition newSource = source.resolveAttributes(resolver);
        ProcessorDefinition newStart = start.resolveAttributes(resolver);
        ProcessorDefinition newLength = length.resolveAttributes(resolver);
        ProcessorDefinition newReplacement = replacement.resolveAttributes(resolver);
        if (newSource == source 
                && newStart == start 
                && newLength == length 
                && newReplacement == replacement) {
            return this;
        }
        return replaceChildren(newSource, newStart, newLength, newReplacement);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return source.supportedByAggsOnlyQuery() 
                && start.supportedByAggsOnlyQuery() 
                && length.supportedByAggsOnlyQuery()
                && replacement.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return source.resolved() && start.resolved() && length.resolved() && replacement.resolved();
    }
    
    protected ProcessorDefinition replaceChildren(ProcessorDefinition newSource, 
            ProcessorDefinition newStart, 
            ProcessorDefinition newLength,
            ProcessorDefinition newReplacement) {
        return new InsertFunctionProcessorDefinition(location(), expression(), newSource, newStart, newLength, newReplacement);
    }

    @Override
    public final void collectFields(SqlSourceBuilder sourceBuilder) {
        source.collectFields(sourceBuilder);
        start.collectFields(sourceBuilder);
        length.collectFields(sourceBuilder);
        replacement.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<InsertFunctionProcessorDefinition> info() {
        return NodeInfo.create(this, InsertFunctionProcessorDefinition::new, expression(), source, start, length, replacement);
    }

    @Override
    public InsertFunctionProcessor asProcessor() {
        return new InsertFunctionProcessor(source.asProcessor(), start.asProcessor(), length.asProcessor(), replacement.asProcessor());
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
    
    public ProcessorDefinition replacement() {
        return replacement;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(source, start, length, replacement);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        InsertFunctionProcessorDefinition other = (InsertFunctionProcessorDefinition) obj;
        return Objects.equals(source, other.source)
                && Objects.equals(start, other.start)
                && Objects.equals(length, other.length)
                && Objects.equals(replacement, other.replacement);
    }
}
