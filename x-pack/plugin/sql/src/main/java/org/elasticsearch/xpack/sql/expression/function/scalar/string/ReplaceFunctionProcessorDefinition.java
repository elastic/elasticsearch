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

public class ReplaceFunctionProcessorDefinition extends ProcessorDefinition {

    private final ProcessorDefinition source, pattern, replacement;

    public ReplaceFunctionProcessorDefinition(Location location, Expression expression, ProcessorDefinition source,
            ProcessorDefinition pattern, ProcessorDefinition replacement) {
        super(location, expression, Arrays.asList(source, pattern, replacement));
        this.source = source;
        this.pattern = pattern;
        this.replacement = replacement;
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
        ProcessorDefinition newPattern = pattern.resolveAttributes(resolver);
        ProcessorDefinition newReplacement = replacement.resolveAttributes(resolver);
        if (newSource == source && newPattern == pattern && newReplacement == replacement) {
            return this;
        }
        return replaceChildren(newSource, newPattern, newReplacement);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return source.supportedByAggsOnlyQuery() && pattern.supportedByAggsOnlyQuery() && replacement.supportedByAggsOnlyQuery();
    }

    @Override
    public boolean resolved() {
        return source.resolved() && pattern.resolved() && replacement.resolved();
    }
    
    protected ProcessorDefinition replaceChildren(ProcessorDefinition newSource, ProcessorDefinition newPattern,
            ProcessorDefinition newReplacement) {
        return new ReplaceFunctionProcessorDefinition(location(), expression(), newSource, newPattern, newReplacement);
    }

    @Override
    public final void collectFields(SqlSourceBuilder sourceBuilder) {
        source.collectFields(sourceBuilder);
        pattern.collectFields(sourceBuilder);
        replacement.collectFields(sourceBuilder);
    }

    @Override
    protected NodeInfo<ReplaceFunctionProcessorDefinition> info() {
        return NodeInfo.create(this, ReplaceFunctionProcessorDefinition::new, expression(), source, pattern, replacement);
    }

    @Override
    public ReplaceFunctionProcessor asProcessor() {
        return new ReplaceFunctionProcessor(source.asProcessor(), pattern.asProcessor(), replacement.asProcessor());
    }
    
    public ProcessorDefinition source() {
        return source;
    }
    
    public ProcessorDefinition pattern() {
        return pattern;
    }
    
    public ProcessorDefinition replacement() {
        return replacement;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(source, pattern, replacement);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ReplaceFunctionProcessorDefinition other = (ReplaceFunctionProcessorDefinition) obj;
        return Objects.equals(source, other.source)
                && Objects.equals(pattern, other.pattern)
                && Objects.equals(replacement, other.replacement);
    }
}
