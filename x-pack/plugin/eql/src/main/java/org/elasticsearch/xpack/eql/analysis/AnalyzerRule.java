/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.rule.Rule;

public abstract class AnalyzerRule<SubPlan extends LogicalPlan> extends Rule<SubPlan, LogicalPlan> {

    // transformUp (post-order) - that is first children and then the node
    // but with a twist; only if the tree is not resolved or analyzed
    @Override
    public final LogicalPlan apply(LogicalPlan plan) {
        return plan.transformUp(t -> t.analyzed() || skipResolved() && t.resolved() ? t : rule(t), typeToken());
    }

    @Override
    protected abstract LogicalPlan rule(SubPlan plan);

    protected boolean skipResolved() {
        return true;
    }
}
