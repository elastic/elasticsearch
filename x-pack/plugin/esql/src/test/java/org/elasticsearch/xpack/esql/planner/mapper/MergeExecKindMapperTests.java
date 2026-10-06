/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;

/**
 * {@code Fork} maps to {@link MergeExec.Kind#FORK} and {@code UnionAll} maps to {@link MergeExec.Kind#UNION}.
 */
public class MergeExecKindMapperTests extends ESTestCase {

    public void testForkMapsToForkKind() {
        PhysicalPlan physical = new Mapper().map(new Versioned<>(fork(), TransportVersion.current()));
        MergeExec merge = as(physical, MergeExec.class);
        assertEquals(MergeExec.Kind.FORK, merge.kind());
    }

    public void testUnionAllMapsToUnionKind() {
        PhysicalPlan physical = new Mapper().map(new Versioned<>(unionAll(), TransportVersion.current()));
        MergeExec merge = as(physical, MergeExec.class);
        assertEquals(MergeExec.Kind.UNION, merge.kind());
    }

    public void testViewUnionAllMapsToUnionKind() {
        PhysicalPlan physical = new Mapper().map(new Versioned<>(viewUnionAll(), TransportVersion.current()));
        MergeExec merge = as(physical, MergeExec.class);
        assertEquals(MergeExec.Kind.UNION, merge.kind());
    }

    public void testBreakPlanIntoSubPlansAndMainPlanReturnsTheMergeKind() {
        PhysicalPlan fork = new Mapper().map(new Versioned<>(fork(), TransportVersion.current()));
        PlannerUtils.SubPlansAndMainPlan forkSplit = PlannerUtils.breakPlanIntoSubPlansAndMainPlan(fork);
        assertEquals(MergeExec.Kind.FORK, forkSplit.kind());
        assertEquals(2, forkSplit.subplans().size());

        PhysicalPlan union = new Mapper().map(new Versioned<>(unionAll(), TransportVersion.current()));
        PlannerUtils.SubPlansAndMainPlan unionSplit = PlannerUtils.breakPlanIntoSubPlansAndMainPlan(union);
        assertEquals(MergeExec.Kind.UNION, unionSplit.kind());
        assertEquals(2, unionSplit.subplans().size());
    }

    private static Fork fork() {
        LogicalPlan left = EsqlTestUtils.emptySource();
        LogicalPlan right = EsqlTestUtils.emptySource();
        return new Fork(Source.EMPTY, List.of(left, right), left.output());
    }

    private static UnionAll unionAll() {
        LogicalPlan left = EsqlTestUtils.emptySource();
        LogicalPlan right = EsqlTestUtils.emptySource();
        return new UnionAll(Source.EMPTY, List.of(left, right), left.output());
    }

    private static ViewUnionAll viewUnionAll() {
        LogicalPlan left = EsqlTestUtils.emptySource();
        LogicalPlan right = EsqlTestUtils.emptySource();
        LinkedHashMap<String, LogicalPlan> children = new LinkedHashMap<>();
        children.put("left", left);
        children.put("right", right);
        return new ViewUnionAll(Source.EMPTY, children, Set.of("left", "right"), left.output());
    }
}
