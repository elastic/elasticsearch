/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.optimizer.cbo.explain.NamedEstimate;
import org.elasticsearch.xpack.esql.optimizer.cbo.stats.PlanStats;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * A family of physical choices that the cost-based optimizer makes and reports on. The explain report, the consumer ledger and the
 * physical plan observer only depend on this interface, so a new kind of decision plugs in without changing them.
 */
public interface DecisionKind {
    /** Stable lowercase name used in reports, for example {@code join_strategy}. */
    String name();

    /** Returns whether {@code plan} is a decision point of this kind. */
    boolean appliesTo(LogicalPlan plan);

    /** Every alternative this kind could choose for {@code plan}, whether or not the search enumerated it. */
    List<String> domain(LogicalPlan plan);

    /**
     * Identifies the decision across the logical and physical representations of {@code plan}. Keys must implement value
     * equality.
     */
    Object key(LogicalPlan plan);

    /** The attributes that identify the decision in plan strings. */
    List<Attribute> anchor(LogicalPlan plan);

    /** The statistics the decision's cost depends on, in a fixed order. */
    List<NamedEstimate> inputs(LogicalPlan plan, Function<LogicalPlan, PlanStats> stats);

    /** Finds the node of {@code plan} that implements the decision identified by {@code key}, if there is one. */
    Optional<Observation> observe(PhysicalPlan plan, Object key);

    /**
     * What a physical plan did with a decision.
     *
     * @param effective the alternative the plan runs
     * @param physicalNode the node that implements it
     * @param observedIn where the node was found, for example {@code coordinator plan}
     */
    record Observation(String effective, String physicalNode, String observedIn) {}
}
