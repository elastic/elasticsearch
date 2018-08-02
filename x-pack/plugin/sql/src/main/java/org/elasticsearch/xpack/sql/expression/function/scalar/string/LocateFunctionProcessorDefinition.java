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

public class LocateFunctionProcessorDefinition extends ProcessorDefinition {

    private final ProcessorDefinition pattern, source, start;

    public LocateFunctionProcessorDefinition(Location location, Expression expression, ProcessorDefinition pattern,
            ProcessorDefinition source, ProcessorDefinition start) {
        super(location, expression, start == null ? Arrays.asList(pattern, source) : Arrays.asList(pattern, source, start));
        this.pattern = pattern;
        this.source = source;
        this.start = start;
    }

    @Override
    public final ProcessorDefinition replaceChildren(List<ProcessorDefinition> newChildren) {
        int childrenSize = newChildren.size();
        if (childrenSize > 3 || childrenSize < 2) {
            throw new IllegalArgumentException("expected [2 or 3] children but received [" + newChildren.size() + "]");
        }
        return replaceChildren(newChildren.get(0), newChildren.get(1), childrenSize == 2 ? null : newChildren.get(2));
    }

    @Override
    public final ProcessorDefinition resolveAttributes(AttributeResolver resolver) {
        ProcessorDefinition newPattern = pattern.resolveAttributes(resolver);
        ProcessorDefinition newSource = source.resolveAttributes(resolver);
        ProcessorDefinition newStart = start == null ? start : start.resolveAttributes(resolver);
        if (newPattern == pattern && newSource == source && newStart == start) {
            return this;
        }
        return replaceChildren(newPattern, newSource, newStart);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return pattern.supportedByAggsOnlyQuery() && source.supportedByAggsOnlyQuery()
                && (start == null || start.supportedByAggsOnlyQuery());
    }

    @Override
    public boolean resolved() {
        return pattern.resolved() && source.resolved() && (start == null || start.resolved());
    }

    protected ProcessorDefinition replaceChildren(ProcessorDefinition newPattern, ProcessorDefinition newSource,
            ProcessorDefinition newStart) {
        return new LocateFunctionProcessorDefinition(location(), expression(), newPattern, newSource, newStart);
    }

    @Override
    public final void collectFields(SqlSourceBuilder sourceBuilder) {
        pattern.collectFields(sourceBuilder);
        source.collectFields(sourceBuilder);
        if (start != null) {
            start.collectFields(sourceBuilder);
        }
    }

    @Override
    protected NodeInfo<LocateFunctionProcessorDefinition> info() {
        return NodeInfo.create(this, LocateFunctionProcessorDefinition::new, expression(), pattern, source, start);
    }

    @Override
    public LocateFunctionProcessor asProcessor() {
        return new LocateFunctionProcessor(pattern.asProcessor(), source.asProcessor(), start == null ? null : start.asProcessor());
    }
    
    public ProcessorDefinition source() {
        return source;
    }
    
    public ProcessorDefinition start() {
        return start;
    }
    
    public ProcessorDefinition pattern() {
        return pattern;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pattern, source, start);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        LocateFunctionProcessorDefinition other = (LocateFunctionProcessorDefinition) obj;
        return Objects.equals(pattern, other.pattern) && Objects.equals(source, other.source) && Objects.equals(start, other.start);
    }
}
