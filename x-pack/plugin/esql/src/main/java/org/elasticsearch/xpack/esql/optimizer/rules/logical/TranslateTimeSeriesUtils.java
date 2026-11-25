/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * Shared helpers for the {@code TranslateTimeSeries*} rules.
 */
final class TranslateTimeSeriesUtils {

    private TranslateTimeSeriesUtils() {}

    /**
     * Applies {@code transformer} to every {@code EsRelation} reachable through the main input path of {@code plan}. The right-hand side
     * of a {@code BinaryPlan} (a lookup index, or an {@code IN}-subquery rewritten to a {@code SemiJoin}) is intentionally skipped: those
     * subtrees are separate time-series sources with their own time-series metadata, which the outer aggregate must not modify. Crossing
     * that boundary would otherwise inject the outer aggregate's metadata into a nested subquery relation that already carries its own,
     * producing a relation with duplicate metadata attributes.
     */
    static LogicalPlan transformTimeSeriesSource(LogicalPlan plan, UnaryOperator<EsRelation> transformer) {
        if (plan instanceof EsRelation relation) {
            return transformer.apply(relation);
        }
        if (plan instanceof BinaryPlan binary) {
            LogicalPlan newLeft = transformTimeSeriesSource(binary.left(), transformer);
            return newLeft == binary.left() ? binary : binary.replaceLeft(newLeft);
        }
        List<LogicalPlan> newChildren = new ArrayList<>(plan.children().size());
        boolean changed = false;
        for (LogicalPlan child : plan.children()) {
            LogicalPlan newChild = transformTimeSeriesSource(child, transformer);
            changed |= newChild != child;
            newChildren.add(newChild);
        }
        return changed ? plan.replaceChildren(newChildren) : plan;
    }
}
