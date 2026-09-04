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
 * @param producerCount how many source producers the fan-in holds in total. Lets a strategy distinguish a read that is
 *                      the whole query from one of many reads running concurrently, which is the difference between a
 *                      transport hop being the dominant cost and the coordinator's own capacity being it. One for a
 *                      single-source query.
 */
public record ExternalDistributionContext(
    PhysicalPlan plan,
    List<ExternalSplit> splits,
    DiscoveryNodes availableNodes,
    QueryPragmas pragmas,
    int producerIndex,
    int producerCount
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
        if (producerCount < 1) {
            throw new IllegalArgumentException("producerCount must be at least one");
        }
        // Catches the two halves being threaded from different places, which would otherwise read as a plausible but
        // wrong picture of the fan-in: an index outside the count describes a producer that does not exist.
        if (producerIndex >= producerCount) {
            throw new IllegalArgumentException(
                "producerIndex [" + producerIndex + "] must be less than producerCount [" + producerCount + "]"
            );
        }
    }

    /** A context for a query with a single external source, which is therefore the only producer. */
    public ExternalDistributionContext(PhysicalPlan plan, List<ExternalSplit> splits, DiscoveryNodes availableNodes, QueryPragmas pragmas) {
        this(plan, splits, availableNodes, pragmas, 0, 1);
    }
}
