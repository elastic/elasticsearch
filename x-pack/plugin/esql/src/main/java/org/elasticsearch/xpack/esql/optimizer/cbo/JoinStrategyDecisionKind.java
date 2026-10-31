/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.NamedEstimate;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.Estimate;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.PlanStats;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.SubqueryLookupJoin;
import org.elasticsearch.xpack.esql.plan.physical.DistributedHashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.JoinStrategySelector.JoinStrategy;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Chooses how a join that can run more than one way is executed: a subquery join is materialized on the coordinator, broadcast, or
 * partitioned, and a lookup-index join runs as a lookup or partitioned.
 */
public final class JoinStrategyDecisionKind implements DecisionKind {
    public static final JoinStrategyDecisionKind INSTANCE = new JoinStrategyDecisionKind();
    public static final String NAME = "join_strategy";

    static final String COORDINATOR_PLAN = "coordinator plan";
    static final String DATA_NODE_FRAGMENT = "data node fragment";

    private static final List<String> SUBQUERY_DOMAIN = List.of(
        JoinStrategy.MATERIALIZED.name(),
        JoinStrategy.BROADCAST.name(),
        JoinStrategy.PARTITIONED.name()
    );
    private static final List<String> LOOKUP_DOMAIN = List.of(JoinStrategy.LOOKUP.name(), JoinStrategy.PARTITIONED.name());

    private JoinStrategyDecisionKind() {}

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public boolean appliesTo(LogicalPlan plan) {
        return plan instanceof SubqueryLookupJoin || plan instanceof Join join && Mapper.isIndexModeLookup(join.right());
    }

    @Override
    public List<String> domain(LogicalPlan plan) {
        return plan instanceof SubqueryLookupJoin ? SUBQUERY_DOMAIN : LOOKUP_DOMAIN;
    }

    @Override
    public JoinKey key(LogicalPlan plan) {
        return JoinKey.of((Join) plan);
    }

    @Override
    public List<Attribute> anchor(LogicalPlan plan) {
        Join join = (Join) plan;
        List<Attribute> anchor = new ArrayList<>(join.config().leftFields());
        anchor.addAll(join.config().rightFields());
        return anchor;
    }

    @Override
    public List<NamedEstimate> inputs(LogicalPlan plan, Function<LogicalPlan, PlanStats> stats) {
        Join join = (Join) plan;
        PlanStats left = stats.apply(join.left());
        PlanStats right = stats.apply(join.right());
        return List.of(
            NamedEstimate.of("left_rows", left == null ? Estimate.UNKNOWN : left.rows()),
            NamedEstimate.of("left_bytes", left == null ? Estimate.UNKNOWN : left.bytes()),
            NamedEstimate.of("right_rows", right == null ? Estimate.UNKNOWN : right.rows()),
            NamedEstimate.of("right_bytes", right == null ? Estimate.UNKNOWN : right.bytes())
        );
    }

    @Override
    public Optional<Observation> observe(PhysicalPlan plan, Object key) {
        List<Observation> found = new ArrayList<>(1);
        plan.forEachDown(node -> {
            if (found.isEmpty() == false) {
                return;
            }
            if (node instanceof DistributedHashJoinExec join && matches(key, join.leftFields(), join.rightFields())) {
                found.add(new Observation(join.mode().name(), join.nodeString(), COORDINATOR_PLAN));
            } else if (node instanceof HashJoinExec join && matches(key, join.leftFields(), join.rightFields())) {
                found.add(new Observation(JoinStrategy.MATERIALIZED.name(), join.nodeString(), COORDINATOR_PLAN));
            } else if (node instanceof LookupJoinExec join && matches(key, join.leftFields(), join.rightFields())) {
                found.add(new Observation(JoinStrategy.LOOKUP.name(), join.nodeString(), COORDINATOR_PLAN));
            } else if (node instanceof FragmentExec fragment) {
                fragment.fragment().forEachDown(Join.class, join -> {
                    if (found.isEmpty() && JoinKey.of(join).equals(key)) {
                        found.add(new Observation(JoinStrategy.LOOKUP.name(), join.nodeString(), DATA_NODE_FRAGMENT));
                    }
                });
            }
        });
        return found.isEmpty() ? Optional.empty() : Optional.of(found.get(0));
    }

    private static boolean matches(Object key, List<Attribute> left, List<Attribute> right) {
        return key.equals(new JoinKey(ids(left), ids(right)));
    }

    private static List<NameId> ids(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::id).toList();
    }
}
