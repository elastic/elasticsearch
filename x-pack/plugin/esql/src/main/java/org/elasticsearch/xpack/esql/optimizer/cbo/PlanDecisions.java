/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.planner.mapper.JoinStrategySelector;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Physical choices made for one logical plan, without adding optimizer state to plan nodes. */
public record PlanDecisions(
    Map<JoinKey, JoinStrategySelector.JoinStrategy> joins,
    Map<AggKey, AggMode> aggregates,
    List<DecisionTrace> traces
) {
    public static final PlanDecisions EMPTY = new PlanDecisions(Map.of(), Map.of(), List.of());

    public PlanDecisions {
        joins = Map.copyOf(joins);
        aggregates = Map.copyOf(aggregates);
        traces = List.copyOf(traces);
    }

    /** Finds the strategy retained for a join after logical and physical mapping. */
    public Optional<JoinStrategySelector.JoinStrategy> join(Join join) {
        return Optional.ofNullable(joins.get(JoinKey.of(join)));
    }

    /** Finds the aggregation mode retained for its physical representation. */
    public Optional<AggMode> aggregate(List<? extends NamedExpression> expressions) {
        return Optional.ofNullable(aggregates.get(AggKey.of(expressions)));
    }
}
