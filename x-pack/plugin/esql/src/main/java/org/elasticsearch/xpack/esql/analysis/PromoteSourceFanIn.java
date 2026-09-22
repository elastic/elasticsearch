/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.SourceFanInUnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;

import static org.elasticsearch.xpack.esql.plan.logical.SourceFanInUnionAll.isSourcePipelineUnary;
import static org.elasticsearch.xpack.esql.plan.logical.SourceFanInUnionAll.producerCount;

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
        LogicalPlan promoted = plan.transformUp(ViewUnionAll.class, PromoteSourceFanIn::promoteOne);
        return promoted.transformDown(SourceFanInUnionAll.class, fanIn -> {
            if (fanIn.children().size() == 1) {
                return fanIn.children().getFirst();
            }
            return fanIn;
        });
    }

    private static LogicalPlan promoteOne(ViewUnionAll view) {
        if (isSourceExpansion(view) == false || view.anyMatch(p -> p instanceof ExternalRelation) == false) {
            return view;
        }
        int producers = 0;
        for (LogicalPlan child : view.children()) {
            producers += producerCount(child);
        }
        if (SourceFanInUnionAll.exceedsMaxProducers(producers)) {
            throw new VerificationException(
                "FROM ["
                    + view.sourceText()
                    + "] resolved to "
                    + producers
                    + " sources, exceeding the current limit of "
                    + SourceFanInUnionAll.MAX_PRODUCERS
                    + " per FROM. Narrow the pattern, exclude some datasets, or split into multiple queries."
            );
        }
        return new SourceFanInUnionAll(view.source(), new ArrayList<>(view.children()), view.output());
    }

    /**
     * Every child is a bare producer ({@link ExternalRelation}, {@link EsRelation}, a nested fan-in,
     * or a {@link Project} over one of those) or a unary pipeline whose leaf is a fan-in.
     */
    private static boolean isSourceExpansion(ViewUnionAll view) {
        for (LogicalPlan child : view.children()) {
            if (isPromotable(child) == false) {
                return false;
            }
        }
        return view.children().isEmpty() == false;
    }

    private static boolean isPromotable(LogicalPlan plan) {
        LogicalPlan current = plan;
        // A Project may wrap a bare relation. Any other unary is promotable only over a fan-in.
        boolean sawNonProjectUnary = false;
        while (isSourcePipelineUnary(current)) {
            if (current instanceof Project == false) {
                sawNonProjectUnary = true;
            }
            current = ((UnaryPlan) current).child();
        }
        if (current instanceof SourceFanInUnionAll) {
            return true;
        }
        if (sawNonProjectUnary) {
            return false;
        }
        return current instanceof ExternalRelation || current instanceof EsRelation;
    }
}
