/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;

public final class PushDownRegexExtract extends OptimizerRules.OptimizerRule<RegexExtract> {
    @Override
    protected LogicalPlan rule(RegexExtract re) {
        return LogicalPlanOptimizer.pushGeneratingPlanPastProjectAndOrderBy(re);
    }
}
