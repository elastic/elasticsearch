/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;

/**
 * Context provided to an {@link ExternalDistributionStrategy} so it can decide
 * how (or whether) to distribute external-source splits across data nodes.
 *
 * @param producerIndex this producer's position among the source producers of a fan-in. Each producer of a fan-in
 *                      discovers splits and plans its distribution on its own, with no view of its siblings, so a
 *                      strategy that always begins at the first eligible node hands every producer's first split to
 *                      that same node. The index gives a strategy a per-producer starting point to spread from.
 *                      Zero for a single-source query, where there is nothing to spread against.
 */
public record ExternalDistributionContext(
    PhysicalPlan plan,
    List<ExternalSplit> splits,
    DiscoveryNodes availableNodes,
    QueryPragmas pragmas,
    int producerIndex
) {
    public ExternalDistributionContext {
        if (plan == null) {
            throw new IllegalArgumentException("plan must not be null");
        }
        if (splits == null) {
            throw new IllegalArgumentException("splits must not be null");
        }
        if (availableNodes == null) {
            throw new IllegalArgumentException("availableNodes must not be null");
        }
        if (pragmas == null) {
            throw new IllegalArgumentException("pragmas must not be null");
        }
        if (producerIndex < 0) {
            throw new IllegalArgumentException("producerIndex must not be negative");
        }
    }

    /** A context for a query with a single external source, which is therefore producer zero. */
    public ExternalDistributionContext(PhysicalPlan plan, List<ExternalSplit> splits, DiscoveryNodes availableNodes, QueryPragmas pragmas) {
        this(plan, splits, availableNodes, pragmas, 0);
    }
}
