/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;

/**
 * A {@link LogicalPlan} that cannot be run only on the data nodes, resp. requires to be at least partially run on the coordinator.
 * When mapping to a physical plan, the first pipeline breaker will give rise to a {@link FragmentExec}
 * that contains the {@link LogicalPlan} that data nodes will execute.
 */
public interface PipelineBreaker {}
