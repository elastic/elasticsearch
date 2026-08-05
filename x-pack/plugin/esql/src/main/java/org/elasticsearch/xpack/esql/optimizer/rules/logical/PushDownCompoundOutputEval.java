/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.CompoundOutputEval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.MandatoryRule;

public final class PushDownCompoundOutputEval extends OptimizerRules.OptimizerRule<CompoundOutputEval<?>> implements MandatoryRule {
    @Override
    protected LogicalPlan rule(CompoundOutputEval<?> coe) {
        return PushDownUtils.pushGeneratingPlanPastProjectAndOrderBy(coe);
    }
}
