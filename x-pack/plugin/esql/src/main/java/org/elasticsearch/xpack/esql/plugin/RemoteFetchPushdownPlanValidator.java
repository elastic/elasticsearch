/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RemoteFetchSourceExec;

/**
 * Validates that a pushdown plan only contains node types supported by remote fetch.
 */
final class RemoteFetchPushdownPlanValidator {
    private RemoteFetchPushdownPlanValidator() {}

    static void validate(PhysicalPlan plan) {
        if (plan == null || plan instanceof RemoteFetchSourceExec) {
            return;
        }
        if (plan instanceof EvalExec || plan instanceof FilterExec || plan instanceof ProjectExec) {
            for (PhysicalPlan child : plan.children()) {
                validate(child);
            }
            return;
        }
        throw new IllegalArgumentException("unsupported remote fetch pushdown plan [" + plan.getClass().getSimpleName() + "]");
    }
}
