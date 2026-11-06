/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.RunReport;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.planner.mapper.JoinStrategySelector;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical choices made for one logical plan, without adding optimizer state to plan nodes.
 *
 * @param joins strategy per join that downstream planners apply
 * @param aggregates distribution mode per aggregate that downstream planners apply
 * @param report explanation of the run, or {@code null} when it was not requested
 * @param points the reported decision points in report order, used to match outcomes against physical plans
 */
public record PlanDecisions(
    Map<JoinKey, JoinStrategySelector.JoinStrategy> joins,
    Map<AggKey, AggMode> aggregates,
    @Nullable RunReport report,
    List<DecisionPoint> points
) {
    public static final PlanDecisions EMPTY = new PlanDecisions(Map.of(), Map.of(), null, List.of());

    public PlanDecisions {
        joins = Map.copyOf(joins);
        aggregates = Map.copyOf(aggregates);
        points = List.copyOf(points);
    }

    /** Finds the strategy retained for a join after logical and physical mapping. */
    public Optional<JoinStrategySelector.JoinStrategy> join(Join join) {
        return Optional.ofNullable(joins.get(JoinKey.of(join)));
    }

    /** Finds the aggregation mode retained for its physical representation. */
    public Optional<AggMode> aggregate(List<? extends NamedExpression> expressions) {
        return Optional.ofNullable(aggregates.get(AggKey.of(expressions)));
    }

    /**
     * A reported decision, linked to the key that consumers and the physical plan observer use to find it.
     *
     * @param id the decision id in the run report, for example {@code d1}
     * @param kind the kind of decision
     * @param key the key returned by {@link DecisionKind#key}
     * @param chosen the chosen alternative
     */
    public record DecisionPoint(String id, DecisionKind kind, Object key, String chosen) {
        public DecisionPoint {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(chosen, "chosen");
        }
    }
}
