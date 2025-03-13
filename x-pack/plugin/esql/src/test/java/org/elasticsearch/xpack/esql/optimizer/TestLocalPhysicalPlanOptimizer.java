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

    private final boolean esRules;

    public TestLocalPhysicalPlanOptimizer(LocalPhysicalOptimizerContext context) {
        this(context, false);
    }

    public TestLocalPhysicalPlanOptimizer(LocalPhysicalOptimizerContext context, boolean esRules) {
        super(context);
        this.esRules = esRules;
    }

    @Override
    protected List<Batch<PhysicalPlan>> batches() {
        return rules(esRules);
    }
}
