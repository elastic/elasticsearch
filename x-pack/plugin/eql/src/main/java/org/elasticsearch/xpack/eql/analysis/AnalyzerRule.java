/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.rule.Rule;

public abstract class AnalyzerRule<SubPlan extends LogicalPlan> extends Rule<SubPlan, LogicalPlan> {

    // transformUp (post-order) - that is first children and then the node
    // but with a twist; only if the tree is not resolved or analyzed
    @Override
    public final LogicalPlan apply(LogicalPlan plan) {
        return plan.transformUp(typeToken(), t -> t.analyzed() || skipResolved() && t.resolved() ? t : rule(t));
    }

    @Override
    protected abstract LogicalPlan rule(SubPlan plan);

    protected boolean skipResolved() {
        return true;
    }
}
