/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * SPI provider interface for supplying additional ESQL plan checks to be performed during verification.
 */
public interface PlanCheckerProvider {
    /**
     * Build a list of checks to perform on the plan. Each one is called once per
     * {@link LogicalPlan} node in the plan.
     */
    List<BiConsumer<LogicalPlan, Failures>> checkers(ProjectResolver projectResolver, ClusterService clusterService);
}
