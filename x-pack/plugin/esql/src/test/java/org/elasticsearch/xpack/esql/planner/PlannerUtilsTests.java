/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class PlannerUtilsTests extends ESTestCase {

    /**
     * Input plan:
     * <pre>
     * MergeExec (outer)
     * ├─ LocalSourceExec (branchA)
     * └─ MergeExec (inner)
     *    ├─ LocalSourceExec (branchB)
     *    └─ LocalSourceExec (branchC)
     * </pre>
     *
     * Expected SubPlan tree:
     * <pre>
     * SubPlan.Merge                  plan = ExchangeSourceExec
     * ├─ SubPlan.Leaf                plan = ExchangeSinkExec → branchA
     * └─ SubPlan.Merge               plan = ExchangeSinkExec → ExchangeSourceExec  (no MergeExec)
     *    ├─ SubPlan.Leaf             plan = ExchangeSinkExec → branchB
     *    └─ SubPlan.Leaf             plan = ExchangeSinkExec → branchC
     * </pre>
     *
     * The outer MergeExec is replaced by ExchangeSourceExec at the root. The inner MergeExec becomes a nested SubPlan.Merge whose plan
     * is ExchangeSinkExec wrapping a new ExchangeSourceExec — it feeds the outer exchange and itself consumes an inner exchange. No
     * MergeExec nodes survive in any plan.
     */
    public void testBuildSubPlanRecursivelyBuildsNestedMerges() {
        List<Attribute> output = List.of(field("a"));
        LocalSourceExec branchA = localSource(output);
        LocalSourceExec branchB = localSource(output);
        LocalSourceExec branchC = localSource(output);
        MergeExec inner = new MergeExec(Source.EMPTY, List.of(branchB, branchC), output);
        MergeExec outer = new MergeExec(Source.EMPTY, List.of(branchA, inner), output);

        var executionPlan = PlannerUtils.buildSubPlan(outer);

        // Root is a Merge whose plan is ExchangeSourceExec (the outer MergeExec was replaced).
        SubPlan.Merge outerMerge = as(executionPlan, SubPlan.Merge.class);
        assertThat(outerMerge.plan(), instanceOf(ExchangeSourceExec.class));
        assertThat(outerMerge.children(), hasSize(2));

        // First child: a Leaf wrapping branchA in an ExchangeSinkExec that writes into the outer exchange.
        SubPlan.Leaf leafA = as(outerMerge.children().get(0), SubPlan.Leaf.class);
        ExchangeSinkExec leafASink = as(leafA.plan(), ExchangeSinkExec.class);
        assertThat(leafASink.child(), sameInstance(branchA));

        // Second child: a nested Merge. Its plan is ExchangeSinkExec → ExchangeSourceExec: it writes into the outer exchange
        // and reads from its own inner exchange. No MergeExec should survive anywhere in this plan.
        SubPlan.Merge innerMerge = as(outerMerge.children().get(1), SubPlan.Merge.class);
        ExchangeSinkExec innerMergeSink = as(innerMerge.plan(), ExchangeSinkExec.class);
        ExchangeSourceExec innerMergeSource = as(innerMergeSink.child(), ExchangeSourceExec.class);

        // The nested Merge's two children are Leaves wrapping branchB and branchC respectively.
        assertThat(innerMerge.children(), hasSize(2));
        SubPlan.Leaf leafB = as(innerMerge.children().get(0), SubPlan.Leaf.class);
        ExchangeSinkExec leafBSink = as(leafB.plan(), ExchangeSinkExec.class);
        assertThat(leafBSink.child(), sameInstance(branchB));
        SubPlan.Leaf leafC = as(innerMerge.children().get(1), SubPlan.Leaf.class);
        ExchangeSinkExec leafCSink = as(leafC.plan(), ExchangeSinkExec.class);
        assertThat(leafCSink.child(), sameInstance(branchC));
    }

    /**
     * Input plan:
     * <pre>
     * LocalSourceExec  (no MergeExec)
     * </pre>
     *
     * Expected SubPlan:
     * <pre>
     * SubPlan.Leaf     plan = LocalSourceExec (unchanged, no wrapping)
     * </pre>
     *
     * A plan with no MergeExec becomes a single Leaf with the original plan unchanged.
     */
    public void testBuildSubPlanWithoutMergeReturnsLeaf() {
        LocalSourceExec plan = localSource(List.of(field("a")));
        var executionPlan = PlannerUtils.buildSubPlan(plan);
        SubPlan.Leaf leaf = as(executionPlan, SubPlan.Leaf.class);
        assertThat(leaf.plan(), sameInstance(plan));
    }

    /**
     * Input plan — a processing command ({@code LIMIT}) sits above the {@code UNION ALL} / {@link MergeExec}:
     * <pre>
     * LimitExec (10)
     * └─ MergeExec
     *    ├─ LocalSourceExec (branchA)
     *    └─ LocalSourceExec (branchB)
     * </pre>
     *
     * Expected SubPlan tree:
     * <pre>
     * SubPlan.Merge          plan = LimitExec(10) → ExchangeSourceExec
     * ├─ SubPlan.Leaf        plan = ExchangeSinkExec → branchA
     * └─ SubPlan.Leaf        plan = ExchangeSinkExec → branchB
     * </pre>
     *
     * The MergeExec is not the root — LimitExec sits above it. {@code buildSubPlan} finds the MergeExec during the traversal,
     * replaces it with ExchangeSourceExec in place, and keeps LimitExec in the coordinator segment plan. At execution time
     * SubPlansExecutor wraps the segment plan in an {@code OutputExec} and runs it locally, so the limit is applied after the
     * exchange source has received all merged data from the two branches.
     */
    public void testBuildSubPlanWithProcessingCommandAboveMerge() {
        List<Attribute> output = List.of(field("a"));
        LocalSourceExec branchA = localSource(output);
        LocalSourceExec branchB = localSource(output);
        MergeExec merge = new MergeExec(Source.EMPTY, List.of(branchA, branchB), output);
        LimitExec limit = new LimitExec(Source.EMPTY, merge, new Literal(Source.EMPTY, 10, DataType.INTEGER), null);

        var executionPlan = PlannerUtils.buildSubPlan(limit);

        // Root is a Merge even though LimitExec is the topmost node in the original plan.
        SubPlan.Merge mergeResult = as(executionPlan, SubPlan.Merge.class);

        // The coordinator segment plan is LimitExec with MergeExec replaced by ExchangeSourceExec.
        LimitExec limitInPlan = as(mergeResult.plan(), LimitExec.class);
        as(limitInPlan.child(), ExchangeSourceExec.class);

        // The two branches of the MergeExec become leaves that write into the exchange source.
        assertThat(mergeResult.children(), hasSize(2));
        ExchangeSinkExec sinkA = as(as(mergeResult.children().get(0), SubPlan.Leaf.class).plan(), ExchangeSinkExec.class);
        assertThat(sinkA.child(), sameInstance(branchA));
        ExchangeSinkExec sinkB = as(as(mergeResult.children().get(1), SubPlan.Leaf.class).plan(), ExchangeSinkExec.class);
        assertThat(sinkB.child(), sameInstance(branchB));
    }

    /**
     * Input plan — one top-level MergeExec with two direct leaf children and two nested MergeExec children, each of which has
     * two leaf children of its own:
     * <pre>
     * MergeExec (top)
     * ├─ LocalSourceExec (leafA)                 ← direct leaf
     * ├─ LocalSourceExec (leafB)                 ← direct leaf
     * ├─ MergeExec (innerA)
     * │  ├─ LocalSourceExec (leafC)
     * │  └─ LocalSourceExec (leafD)
     * └─ MergeExec (innerB)
     *    ├─ LocalSourceExec (leafE)
     *    └─ LocalSourceExec (leafF)
     * </pre>
     *
     * Expected SubPlan tree:
     * <pre>
     * SubPlan.Merge                  plan = ExchangeSourceExec
     * ├─ SubPlan.Leaf                plan = ExchangeSinkExec → leafA
     * ├─ SubPlan.Leaf                plan = ExchangeSinkExec → leafB
     * ├─ SubPlan.Merge               plan = ExchangeSinkExec → ExchangeSourceExec
     * │  ├─ SubPlan.Leaf             plan = ExchangeSinkExec → leafC
     * │  └─ SubPlan.Leaf             plan = ExchangeSinkExec → leafD
     * └─ SubPlan.Merge               plan = ExchangeSinkExec → ExchangeSourceExec
     *    ├─ SubPlan.Leaf             plan = ExchangeSinkExec → leafE
     *    └─ SubPlan.Leaf             plan = ExchangeSinkExec → leafF
     * </pre>
     *
     * This is the general mixed case: a single coordinator merge that aggregates both direct producers and nested coordinator segments.
     * The two direct leaves become {@link SubPlan.Leaf} children directly; each nested {@link MergeExec} becomes a {@link SubPlan.Merge}
     * child with its own {@link ExchangeSourceExec}–{@link ExchangeSinkExec} pair.
     */
    public void testBuildSubPlanWithMixedLeavesAndNestedMerges() {
        List<Attribute> output = List.of(field("a"));
        LocalSourceExec leafA = localSource(output);
        LocalSourceExec leafB = localSource(output);
        LocalSourceExec leafC = localSource(output);
        LocalSourceExec leafD = localSource(output);
        LocalSourceExec leafE = localSource(output);
        LocalSourceExec leafF = localSource(output);
        MergeExec innerA = new MergeExec(Source.EMPTY, List.of(leafC, leafD), output);
        MergeExec innerB = new MergeExec(Source.EMPTY, List.of(leafE, leafF), output);
        MergeExec top = new MergeExec(Source.EMPTY, List.of(leafA, leafB, innerA, innerB), output);

        var executionPlan = PlannerUtils.buildSubPlan(top);

        // Root is a Merge with four children: two leaves, two nested merges.
        SubPlan.Merge topMerge = as(executionPlan, SubPlan.Merge.class);
        as(topMerge.plan(), ExchangeSourceExec.class);
        assertThat(topMerge.children(), hasSize(4));

        // Children 0 and 1 are direct leaves wrapping leafA and leafB.
        ExchangeSinkExec sinkA = as(as(topMerge.children().get(0), SubPlan.Leaf.class).plan(), ExchangeSinkExec.class);
        assertThat(sinkA.child(), sameInstance(leafA));
        ExchangeSinkExec sinkB = as(as(topMerge.children().get(1), SubPlan.Leaf.class).plan(), ExchangeSinkExec.class);
        assertThat(sinkB.child(), sameInstance(leafB));

        // Child 2 is a nested Merge for innerA: plan = ExchangeSinkExec → ExchangeSourceExec, two leaf children.
        SubPlan.Merge nestedA = as(topMerge.children().get(2), SubPlan.Merge.class);
        as(as(nestedA.plan(), ExchangeSinkExec.class).child(), ExchangeSourceExec.class);
        assertThat(nestedA.children(), hasSize(2));
        ExchangeSinkExec sinkC = as(as(nestedA.children().get(0), SubPlan.Leaf.class).plan(), ExchangeSinkExec.class);
        assertThat(sinkC.child(), sameInstance(leafC));
        ExchangeSinkExec sinkD = as(as(nestedA.children().get(1), SubPlan.Leaf.class).plan(), ExchangeSinkExec.class);
        assertThat(sinkD.child(), sameInstance(leafD));

        // Child 3 is a nested Merge for innerB: same structure, two leaf children.
        SubPlan.Merge nestedB = as(topMerge.children().get(3), SubPlan.Merge.class);
        as(as(nestedB.plan(), ExchangeSinkExec.class).child(), ExchangeSourceExec.class);
        assertThat(nestedB.children(), hasSize(2));
        ExchangeSinkExec sinkE = as(as(nestedB.children().get(0), SubPlan.Leaf.class).plan(), ExchangeSinkExec.class);
        assertThat(sinkE.child(), sameInstance(leafE));
        ExchangeSinkExec sinkF = as(as(nestedB.children().get(1), SubPlan.Leaf.class).plan(), ExchangeSinkExec.class);
        assertThat(sinkF.child(), sameInstance(leafF));
    }

    /**
     * Input plan — two MergeExecs in sibling branches of a binary operator (neither nested under the other):
     * <pre>
     * HashJoinExec
     * ├─ MergeExec (left)
     * └─ MergeExec (right)
     * </pre>
     *
     * In practice this shape cannot appear: when a join's lookup side is a {@code UNION ALL}, it is fully materialised before the join
     * executes, so the right side is never a {@link MergeExec} by the time {@code buildSubPlan} is called. The restriction is kept
     * because one compute context supplies one exchange source to all {@link ExchangeSourceExec} nodes in the segment — two sibling
     * merges would each need a separate exchange source, which the current execution model does not support.
     */
    public void testBuildSubPlanRejectsSiblingTopmostMerges() {
        List<Attribute> output = List.of(field("a"));
        MergeExec first = new MergeExec(Source.EMPTY, List.of(localSource(output), localSource(output)), output);
        MergeExec second = new MergeExec(Source.EMPTY, List.of(localSource(output), localSource(output)), output);
        HashJoinExec siblingContainer = new HashJoinExec(Source.EMPTY, first, second, List.of(), List.of(), List.of());

        var exception = expectThrows(EsqlIllegalArgumentException.class, () -> PlannerUtils.buildSubPlan(siblingContainer));
        assertThat(exception.getMessage(), containsString("expected a single topmost MergeExec"));
    }

    private static FieldAttribute field(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            name,
            new EsField(name, DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private static LocalSourceExec localSource(List<Attribute> output) {
        return new LocalSourceExec(Source.EMPTY, output, EmptyLocalSupplier.EMPTY);
    }
}
