/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.capabilities.Unresolvable;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineFilters;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.tree.EsqlNodeSubclassTests;

import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.Matchers.empty;

/**
 * Makes whoever adds a {@link LogicalPlan} type decide how QSTR and KQL treat it.
 * <p>
 * {@link FullTextFunction} rejects QSTR/KQL after a command that introduces a column named like an indexed field, because
 * the query string names its fields inside an opaque string and {@link PushDownAndCombineFilters} can push the filter below
 * such a command. That check only knows the plan types it was written for, and a type it doesn't know is silently treated
 * as introducing nothing. This test fails for every concrete plan type that the check doesn't cover and that isn't listed
 * here as safe.
 * <p>
 * If it fails for your new type:
 * <ul>
 *     <li>if it appends output columns, make it a {@link GeneratingPlan}, which the check picks up on its own;</li>
 *     <li>if it renames or joins in columns another way, add a branch to {@code FullTextFunction#asShadowingCommand};</li>
 *     <li>if it introduces no column, or QSTR/KQL are always rejected after it or can't be pushed below it, add it to the
 *     matching set below.</li>
 * </ul>
 * Pushing a filter past a type means adding it to {@link PushDownAndCombineFilters}. Do that for a type listed here only
 * after adding the shadowing check for it.
 */
public class FullTextShadowingCoverageTests extends ESTestCase {

    /**
     * Rewritten into other plan types before verification, so the check never sees them.
     */
    private static final Set<String> REPLACED_BEFORE_VERIFICATION = Set.of("Drop", "FillNull", "Lookup", "Rename");

    /**
     * QSTR/KQL are rejected after these by the position check, so a shadowed name is never reached.
     */
    private static final Set<String> REJECTED_BY_POSITION_CHECK = Set.of(
        "Aggregate",
        "BranchPlan",
        "ChangePoint",
        "Dedup",
        "ExternalRelation",
        "Fork",
        "InlineStats",
        "Limit",
        "LimitBy",
        "MvExpand",
        "Row",
        "SampledAggregate",
        "ShowInfo",
        "TimeSeriesAggregate",
        "TopN",
        "TopNBy",
        "UnionAll"
    );

    /**
     * {@link PushDownAndCombineFilters} has no branch for these, so a QSTR/KQL filter is never pushed below them and
     * redefining a name there can't change which field the query string reads. Sources and sorts that keep the names of
     * their input are here too.
     */
    private static final Set<String> FILTER_NOT_PUSHED_PAST = Set.of(
        "Explain",
        "EsRelation",
        "Filter",
        "Fuse",
        "FuseScoreEval",
        "InsertEmptyBuckets",
        "LocalRelation",
        "MMR",
        "MetricsInfo",
        "NamedSubquery",
        "OrderBy",
        "PackDims",
        "ParameterizedQuery",
        "Sample",
        "SparklineGenerateEmptyBuckets",
        "StubRelation",
        "Subquery",
        "TimeSeriesCollapse",
        "TopNPreFilter",
        "TsInfo",
        "UnpackDims"
    );

    public void testEveryLogicalPlanIsClassifiedForShadowing() throws Exception {
        Set<String> listed = new HashSet<>();
        listed.addAll(REPLACED_BEFORE_VERIFICATION);
        listed.addAll(REJECTED_BY_POSITION_CHECK);
        listed.addAll(FILTER_NOT_PUSHED_PAST);

        Set<String> unclassified = new TreeSet<>();
        Set<String> seen = new HashSet<>();
        for (Class<? extends LogicalPlan> plan : EsqlNodeSubclassTests.subclassesOf(LogicalPlan.class)) {
            if (Modifier.isAbstract(plan.getModifiers()) || Unresolvable.class.isAssignableFrom(plan)
            // PromQL has its own plan tree, which a filter outside it is never pushed into.
                || plan.getName().contains(".promql.")) {
                continue;
            }
            seen.add(plan.getSimpleName());
            boolean checked = GeneratingPlan.class.isAssignableFrom(plan)
                || Project.class.isAssignableFrom(plan)
                || Join.class.isAssignableFrom(plan);
            if (checked == false && listed.contains(plan.getSimpleName()) == false) {
                unclassified.add(plan.getName());
            }
        }
        assertThat(
            "These plan types are not covered by the QSTR/KQL shadowing check. See the class Javadoc: " + unclassified,
            unclassified,
            empty()
        );

        Set<String> stale = new TreeSet<>(listed);
        stale.removeAll(seen);
        assertThat("These names no longer match a plan type, remove them: " + stale, stale, empty());
    }
}
