/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

public class PushLimitToSource extends PhysicalOptimizerRules.OptimizerRule<LimitExec> {
    @Override
    protected PhysicalPlan rule(LimitExec limitExec) {
        PhysicalPlan plan = limitExec;
        PhysicalPlan child = limitExec.child();
        if (child instanceof EsQueryExec queryExec) { // add_task_parallelism_above_query: false
            plan = queryExec.withLimit(limitExec.limit());
        } else if (child instanceof ExchangeExec exchangeExec && exchangeExec.child() instanceof EsQueryExec queryExec) {
            plan = exchangeExec.replaceChild(queryExec.withLimit(limitExec.limit()));
        }
        return plan;
    }
}
