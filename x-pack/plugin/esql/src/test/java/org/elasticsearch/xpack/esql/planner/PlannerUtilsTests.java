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
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class PlannerUtilsTests extends ESTestCase {

    /**
     * {@link PlannerUtils#breakPlanIntoSubPlansAndMainPlan} must only break the topmost {@link MergeExec}: a
     * nested {@link MergeExec} (from a nested subquery) stays intact inside the collected sub plan, and is
     * peeled off by the next {@code breakPlanIntoSubPlansAndMainPlan} call when that sub plan is executed
     * (see {@code MergeLevelExecutor}).
     */
    public void testBreakPlanBreaksTopmostMergeOnly() {
        List<Attribute> output = List.of(field("a"));
        LocalSourceExec branchA = localSource(output);
        LocalSourceExec branchB = localSource(output);
        LocalSourceExec branchC = localSource(output);
        MergeExec inner = new MergeExec(Source.EMPTY, List.of(branchB, branchC), output);
        MergeExec outer = new MergeExec(Source.EMPTY, List.of(branchA, inner), output);

        var broken = PlannerUtils.breakPlanIntoSubPlansAndMainPlan(outer);

        // main plan: an exchange source in place of the outer merge, no MergeExec left
        assertThat(broken.v2(), instanceOf(ExchangeSourceExec.class));

        // one sub plan per outer branch, each wrapped in an exchange sink; the nested merge stays intact
        List<PhysicalPlan> subplans = broken.v1();
        assertThat(subplans, hasSize(2));
        assertThat(subplans.get(0), instanceOf(ExchangeSinkExec.class));
        assertThat(((ExchangeSinkExec) subplans.get(0)).child(), sameInstance(branchA));
        assertThat(subplans.get(1), instanceOf(ExchangeSinkExec.class));
        assertThat(((ExchangeSinkExec) subplans.get(1)).child(), sameInstance(inner));

        // breaking the nested sub plan peels the next level: the inner branches become sub plans and the
        // inner merge is replaced by an exchange source below the sink
        var nested = PlannerUtils.breakPlanIntoSubPlansAndMainPlan(subplans.get(1));
        assertThat(nested.v1(), hasSize(2));
        assertThat(((ExchangeSinkExec) nested.v1().get(0)).child(), sameInstance(branchB));
        assertThat(((ExchangeSinkExec) nested.v1().get(1)).child(), sameInstance(branchC));
        assertThat(nested.v2(), instanceOf(ExchangeSinkExec.class));
        assertThat(((ExchangeSinkExec) nested.v2()).child(), instanceOf(ExchangeSourceExec.class));
        assertThat(nested.v2().anyMatch(MergeExec.class::isInstance), is(false));
    }

    public void testBreakPlanWithoutMergeReturnsPlanUnchanged() {
        LocalSourceExec plan = localSource(List.of(field("a")));
        var broken = PlannerUtils.breakPlanIntoSubPlansAndMainPlan(plan);
        assertThat(broken.v1(), empty());
        assertThat(broken.v2(), sameInstance(plan));
    }

    public void testBreakPlanRejectsMultipleTopmostMerges() {
        List<Attribute> output = List.of(field("a"));
        MergeExec first = new MergeExec(Source.EMPTY, List.of(localSource(output), localSource(output)), output);
        MergeExec second = new MergeExec(Source.EMPTY, List.of(localSource(output), localSource(output)), output);
        HashJoinExec siblingContainer = new HashJoinExec(Source.EMPTY, first, second, List.of(), List.of(), List.of());

        var exception = expectThrows(
            EsqlIllegalArgumentException.class,
            () -> PlannerUtils.breakPlanIntoSubPlansAndMainPlan(siblingContainer)
        );
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
