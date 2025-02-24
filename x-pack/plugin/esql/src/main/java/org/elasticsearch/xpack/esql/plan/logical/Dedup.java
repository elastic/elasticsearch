/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Dedup extends UnaryPlan implements SurrogateLogicalPlan {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Dedup", Dedup::new);
    private final List<NamedExpression> aggregates;
    private List<Attribute> lazyOutput;

    public Dedup(Source source, LogicalPlan child, List<NamedExpression> aggregates) {
        super(source, child);
        this.aggregates = aggregates;
    }

    private Dedup(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteableCollectionAsList(NamedExpression.class)
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Dedup::new, child(), aggregates);
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(aggregates);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Dedup(source(), newChild, aggregates);
    }

    @Override
    public LogicalPlan surrogate() {
        List<NamedExpression> aggs = new ArrayList<>(aggregates);
        List<Expression> groupings = new ArrayList<>();

        Set<String> names = new HashSet<>(aggs.stream().map(exp -> exp.name()).toList());

        for (Attribute attr : child().output()) {
            if (names.contains(attr.name())) {
                continue;
            }
            aggs.add(attr);
            groupings.add(attr);
        }

        return new Aggregate(source(), child(), Aggregate.AggregateType.STANDARD, groupings, aggs);
    }

    public List<NamedExpression> aggregates() {
        return aggregates;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = NamedExpressions.mergeOutputAttributes(aggregates, child().output());
        }
        return lazyOutput;
    }
}
