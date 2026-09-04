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
 */
public record ExternalDistributionContext(
    PhysicalPlan plan,
    List<ExternalSplit> splits,
    DiscoveryNodes availableNodes,
    QueryPragmas pragmas
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
    }
}
