/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;

public class TestLocalPhysicalPlanOptimizer extends LocalPhysicalPlanOptimizer {

    public TestLocalPhysicalPlanOptimizer(LocalPhysicalOptimizerContext context) {
        super(context);
    }

    @Override
    protected List<Batch<PhysicalPlan>> batches() {
        return rules(false);
    }
}
