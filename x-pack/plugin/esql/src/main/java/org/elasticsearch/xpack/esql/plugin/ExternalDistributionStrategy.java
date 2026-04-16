/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

/**
 * Decides whether an external source query should be distributed across data nodes
 * or executed locally on the coordinator.
 */
public interface ExternalDistributionStrategy {

    ExternalDistributionPlan planDistribution(ExternalDistributionContext context);
}
