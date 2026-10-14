/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * A lone metadata projection needs no proxy alias to distinguish it from other projections.
 * Remove that alias before translating time-series aggregates, retaining its identity so parent references stay valid.
 */
public final class RemoveRedundantTimeSeriesAliases extends Rule<LogicalPlan, LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        Set<NameId> renamed = new HashSet<>();
        LogicalPlan result = plan.transformUp(TimeSeriesAggregate.class, aggregate -> {
            var metadata = aggregate.child().output().stream().filter(TimeSeriesMetadataAttribute.class::isInstance).toList();
            if (metadata.size() != 1) {
                return aggregate;
            }
            var metadataGroupings = aggregate.groupings()
                .stream()
                .filter(g -> Alias.unwrap(g) instanceof TimeSeriesMetadataAttribute)
                .toList();
            if (metadataGroupings.size() != 1
                || metadataGroupings.getFirst() instanceof Alias == false
                || Alias.unwrap(metadataGroupings.getFirst()).semanticEquals(metadata.getFirst()) == false) {
                return aggregate;
            }
            Alias alias = (Alias) metadataGroupings.getFirst();
            Attribute field = metadata.getFirst();
            Attribute replacement = field.withId(alias.id());
            var groupings = new ArrayList<Expression>(aggregate.groupings());
            groupings.replaceAll(g -> g == alias ? replacement : g);
            LogicalPlan child = aggregate.child()
                .transformExpressionsDown(Attribute.class, a -> a.id().equals(field.id()) ? replacement : a);
            renamed.add(alias.id());
            return aggregate.with(child, groupings, aggregate.aggregates());
        });
        // References may occur above the aggregate, including in filters and projections.
        return renamed.isEmpty()
            ? result
            : result.transformExpressionsDown(
                Attribute.class,
                a -> renamed.contains(a.id()) ? a.withName(MetadataAttribute.TIMESERIES) : a
            );
    }
}
