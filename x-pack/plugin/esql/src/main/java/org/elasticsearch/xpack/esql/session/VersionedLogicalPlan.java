/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

/**
 * A wrapper for a logical plan after analysis together with the minimum transport versions of all nodes
 * that hold the indices involved in the query. This is used to ensure that the physical plan
 * generated from the logical plan is compatible with all nodes in the cluster.
 */
public record VersionedLogicalPlan(LogicalPlan analyzedPlan, TransportVersion minimumVersion) {
}
