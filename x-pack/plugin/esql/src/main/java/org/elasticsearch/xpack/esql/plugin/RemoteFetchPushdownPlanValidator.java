/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RemoteFetchSource;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

/**
 * Validates that a pushdown plan only contains fragment node types supported by remote fetch.
 */
final class RemoteFetchPushdownPlanValidator {
    private RemoteFetchPushdownPlanValidator() {}

    static void validate(PhysicalPlan plan) {
        if (plan == null) {
            return;
        }
        if (plan instanceof FragmentExec fragmentExec) {
            validateFragment(fragmentExec.fragment());
            return;
        }
        throw new IllegalArgumentException("unsupported remote fetch pushdown plan [" + plan.getClass().getSimpleName() + "]");
    }

    private static void validateFragment(LogicalPlan plan) {
        if (plan instanceof RemoteFetchSource) {
            return;
        }
        if (plan instanceof Eval || plan instanceof Filter || plan instanceof Project) {
            for (LogicalPlan child : plan.children()) {
                validateFragment(child);
            }
            return;
        }
        throw new IllegalArgumentException("unsupported remote fetch pushdown plan [" + plan.getClass().getSimpleName() + "]");
    }
}
