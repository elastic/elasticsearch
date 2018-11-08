/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.sql.capabilities.Resolvables;
import org.elasticsearch.xpack.sql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class InPipe extends Pipe {

    private List<Pipe> pipes;

    public InPipe(Location location, Expression expression, List<Pipe> pipes) {
        super(location, expression, pipes);
        this.pipes = pipes;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        if (newChildren.size() < 2) {
            throw new IllegalArgumentException("expected at least [2] children but received [" + newChildren.size() + "]");
        }
        return new InPipe(location(), expression(), newChildren);
    }

    @Override
    protected NodeInfo<InPipe> info() {
        return NodeInfo.create(this, InPipe::new, expression(), pipes);
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return pipes.stream().allMatch(FieldExtraction::supportedByAggsOnlyQuery);
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        List<Pipe> newPipes = new ArrayList<>(pipes.size());
        for (Pipe p : pipes) {
            newPipes.add(p.resolveAttributes(resolver));
        }
        return replaceChildren(newPipes);
    }

    @Override
    public boolean resolved() {
        return Resolvables.resolved(pipes);
    }

    @Override
    public final void collectFields(SqlSourceBuilder sourceBuilder) {
        pipes.forEach(p -> p.collectFields(sourceBuilder));
    }

    @Override
    public int hashCode() {
        return Objects.hash(pipes);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        InPipe other = (InPipe) obj;
        return Objects.equals(pipes, other.pipes);
    }

    @Override
    public InProcessor asProcessor() {
        return new InProcessor(pipes.stream().map(Pipe::asProcessor).collect(Collectors.toList()));
    }
}
