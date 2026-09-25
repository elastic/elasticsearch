/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.SourceFanInUnionAll;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;

/**
 * Turns a {@link ViewUnionAll} that is only a resolved {@code FROM} into a {@link SourceFanInUnionAll}.
 * <p>
 * A view whose body is datasets, indices, or a mix, including a {@code WHERE} (or another unary
 * pipeline) on that expansion beside a matched namesake, is the same source list the dataset
 * rewriter builds. An index-only view union stays a {@link ViewUnionAll}. A {@code FORK}, a join,
 * a subquery, or a union the user wrote is not a source list and is left alone.
 */
public final class PromoteSourceFanIn extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return promote(plan);
    }

    /**
     * Promotes every source-expansion view union, merges sibling index reads in every fan-in (see
     * {@link SourceFanInUnionAll#withIndexReadsCollapsed}), and collapses any single-child fan-in that results.
     */
    public static LogicalPlan promote(LogicalPlan plan) {
        LogicalPlan promoted = plan.transformUp(ViewUnionAll.class, PromoteSourceFanIn::promoteOne);
        return collapseSingleChildFanIns(promoted.transformUp(SourceFanInUnionAll.class, SourceFanInUnionAll::withIndexReadsCollapsed));
    }

    private static LogicalPlan collapseSingleChildFanIns(LogicalPlan plan) {
        return plan.transformDown(SourceFanInUnionAll.class, fanIn -> {
            if (fanIn.children().size() == 1) {
                return fanIn.children().getFirst();
            }
            return fanIn;
        });
    }

    private static LogicalPlan promoteOne(ViewUnionAll view) {
        if (SourceFanInUnionAll.isSourceExpansion(view) == false) {
            return view;
        }
        return new SourceFanInUnionAll(view.source(), new ArrayList<>(view.children()), view.output());
    }
}
