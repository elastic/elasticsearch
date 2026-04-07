/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;

import java.util.List;
import java.util.Map;

/**
 * The result of an {@link ExternalDistributionStrategy} decision: whether to distribute
 * and, if so, which splits go to which node.
 *
 * @param nodeAssignments mapping from node id to the splits assigned to that node; empty when not distributed
 * @param distributed     true when the query should be fanned out to data nodes
 */
public record ExternalDistributionPlan(Map<String, List<ExternalSplit>> nodeAssignments, boolean distributed) {

    public static final ExternalDistributionPlan LOCAL = new ExternalDistributionPlan(Map.of(), false);
}
