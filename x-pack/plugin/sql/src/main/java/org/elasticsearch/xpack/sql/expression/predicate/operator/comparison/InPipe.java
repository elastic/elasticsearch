/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.comparison;

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
import java.util.stream.Stream;

public class InPipe extends Pipe {

    private Pipe left;
    private List<Pipe> right;

    public InPipe(Location location, Expression expression, Pipe left, List<Pipe> right) {
        super(location, expression, Stream.concat(Stream.of(left), right.stream()).collect(Collectors.toList()));
        this.left = left;
        this.right = right;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        if (newChildren.size() < 2) {
            throw new IllegalArgumentException("expected at least [2] children but received [" + newChildren.size() + "]");
        }
        left = newChildren.get(0);
        return new InPipe(location(), expression(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    protected NodeInfo<InPipe> info() {
        return NodeInfo.create(this, InPipe::new, expression(), left, right);
    }

    public Pipe left() {
        return left;
    }

    public List<Pipe> right() {
        return right;
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return left.supportedByAggsOnlyQuery() && right.stream().allMatch(FieldExtraction::supportedByAggsOnlyQuery);
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newLeft = left.resolveAttributes(resolver);
        List<Pipe> newRight = new ArrayList<>(right.size());
        for (Pipe p : right) {
            newRight.add(p.resolveAttributes(resolver));
        }
        return replaceChildren(Stream.concat(Stream.of(newLeft), newRight.stream()).collect(Collectors.toList()));
    }

    @Override
    public boolean resolved() {
        return left().resolved() && right().stream().allMatch(Pipe::resolved);
    }

    @Override
    public final void collectFields(SqlSourceBuilder sourceBuilder) {
        left.collectFields(sourceBuilder);
        right.forEach(p -> p.collectFields(sourceBuilder));
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

        InPipe other = (InPipe) obj;
        return Objects.equals(left(), other.left())
            && Objects.equals(right(), other.right());
    }

    @Override
    public InProcessor asProcessor() {
        return new InProcessor(left().asProcessor(), right().stream().map(Pipe::asProcessor).collect(Collectors.toList()));
    }
}
