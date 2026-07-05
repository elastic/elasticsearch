/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

/**
 * Always executes external sources on the coordinator — no distribution.
 */
public final class CoordinatorOnlyStrategy implements ExternalDistributionStrategy {

    public static final CoordinatorOnlyStrategy INSTANCE = new CoordinatorOnlyStrategy();

    private CoordinatorOnlyStrategy() {}

    @Override
    public ExternalDistributionPlan planDistribution(ExternalDistributionContext context) {
        return ExternalDistributionPlan.LOCAL;
    }
}
