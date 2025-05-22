/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Removes the rows that contain the same values for a list specified fields.
 * Dedup also receives a list of aggregates similar to {@link Aggregate STATS}.
 * In the current implementation Dedup implements {@link SurrogateLogicalPlan} and actually expands to {@link Aggregate STATS}.
 * At the moment this is only used in the planning of the RRF command, but could evolve as a standalone command.
 */
public class Dedup extends UnaryPlan implements SurrogateLogicalPlan {
    private final List<NamedExpression> aggregates;
    private final List<Attribute> groupings;
    private List<Attribute> lazyOutput;
    private List<NamedExpression> lazyFinalAggs;

    public Dedup(Source source, LogicalPlan child, List<NamedExpression> aggregates, List<Attribute> groupings) {
        super(source, child);
        this.aggregates = aggregates;
        this.groupings = groupings;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Dedup::new, child(), aggregates, groupings);
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(aggregates) && Resolvables.resolved(groupings);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Dedup(source(), newChild, aggregates, groupings);
    }

    @Override
    public LogicalPlan surrogate() {
        return new Aggregate(source(), child(), new ArrayList<>(groupings), finalAggs());
    }

    public List<NamedExpression> aggregates() {
        return aggregates;
    }

    public List<Attribute> groupings() {
        return groupings;
    }

    public List<NamedExpression> finalAggs() {
        if (lazyFinalAggs == null) {
            lazyFinalAggs = new ArrayList<>(aggregates);

            Set<String> names = new HashSet<>(aggregates.stream().map(att -> att.name()).toList());
            Expression aggFilter = new Literal(source(), true, DataType.BOOLEAN);

            for (Attribute attr : child().output()) {
                if (names.contains(attr.name())) {
                    continue;
                }

                lazyFinalAggs.add(new Alias(source(), attr.name(), new Values(source(), attr, aggFilter)));
            }
        }

        return lazyFinalAggs;
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = NamedExpressions.mergeOutputAttributes(finalAggs(), child().output());
        }
        return lazyOutput;
    }
}
