/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

/**
 * Data structure to pass allocation statistics between the desired balance classes.
 */
public record ClusterAllocationStats(long unassignedShards, long totalAllocations, long undesiredAllocationsExcludingShuttingDownNodes) {
    public static final ClusterAllocationStats EMPTY_ALLOCATION_STATS = new ClusterAllocationStats(-1, -1, -1);
};
