/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.util.Holder;

import java.util.List;

/**
 * NOCOMMIT javadocs
 */
public interface Phased {
    LogicalPlan firstPhase();
    LogicalPlan nextPhase(List<Attribute> layout, List<Page> firstPhaseResult);

    static LogicalPlan extractNextPhase(LogicalPlan plan) {
        var firstPhase = new Holder<LogicalPlan>();
        plan.forEachUp(t -> {
            if (t instanceof Phased phased) {
                firstPhase.set(phased.firstPhase());
            }
        });
        return firstPhase.get();
    }

    static LogicalPlan applyResultsFromNextPhase(LogicalPlan plan, List<Attribute> layout, List<Page> result) {
        return plan.transformUp(logicalPlan -> {
            // NOCOMMIT make sure this stops after the first one.
            if (logicalPlan instanceof Phased phased) {
                return phased.nextPhase(layout, result);
            }
            return logicalPlan;
        });
    }
}
