/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * A Fork is a {@code Plan} with one child, but holds several logical subplans, e.g.
 * {@code FORK [WHERE content:"fox" ] [WHERE content:"dog"] }
 */
public class Fork extends UnaryPlan implements SurrogateLogicalPlan {

    private final List<LogicalPlan> subPlans;
    List<Attribute> lazyOutput;

    public Fork(Source source, LogicalPlan child, List<LogicalPlan> subPlans) {
        super(source, child);
        if (subPlans.size() < 2) {
            throw new IllegalArgumentException("requires more than two subqueries, got:" + subPlans.size());
        }
        this.subPlans = subPlans;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    public List<LogicalPlan> subPlans() {
        return subPlans;
    }

    @Override
    public LogicalPlan surrogate() {
        var newChildren = subPlans.stream().map(p -> Merge.replaceStub(child(), p)).toList();
        return new Merge(source(), newChildren);
    }

    @Override
    public boolean expressionsResolved() {
        return child().resolved() && subPlans.stream().allMatch(LogicalPlan::resolved);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Fork::new, child(), subPlans);
    }

    static List<LogicalPlan> replaceEmptyStubs(List<LogicalPlan> subQueries, LogicalPlan newChild) {
        List<Attribute> attributes = List.copyOf(newChild.output());
        return subQueries.stream().map(subquery -> {
            var newStub = new StubRelation(subquery.source(), attributes);
            return subquery.transformUp(StubRelation.class, stubRelation -> newStub);
        }).toList();
    }

    @Override
    public Fork replaceChild(LogicalPlan newChild) {
        var newSubQueries = replaceEmptyStubs(subPlans, newChild);
        return new Fork(source(), newChild, newSubQueries);
    }

    public Fork replaceSubPlans(List<LogicalPlan> subPlans) {
        return new Fork(source(), child(), subPlans);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = Stream.concat(children().getFirst().output().stream(), subPlans.getFirst().output().stream()).distinct().toList();
        }
        return lazyOutput;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), subPlans);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        Fork other = (Fork) o;
        return Objects.equals(subPlans, other.subPlans);
    }
}
