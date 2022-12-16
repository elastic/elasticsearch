/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * Scope marked used as a delimiter inside the plan.
 * Currently used to demarcate a per-segment local plan.
 */
public class LocalPlanExec extends UnaryExec {

    public LocalPlanExec(Source source, PhysicalPlan child) {
        super(source, child);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new LocalPlanExec(source(), newChild);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, LocalPlanExec::new, child());
    }
}
