/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;

import java.util.function.BooleanSupplier;

public class ArchiveAllocationDecider extends AllocationDecider {

    static final String NAME = "archive";

    private final BooleanSupplier hasValidLicenseSupplier;

    public ArchiveAllocationDecider(BooleanSupplier hasValidLicenseSupplier) {
        this.hasValidLicenseSupplier = hasValidLicenseSupplier;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return allowAllocation(allocation.metadata().getIndexSafe(shardRouting.index()), allocation);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        return allowAllocation(allocation.metadata().getIndexSafe(shardRouting.index()), allocation);
    }

    @Override
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        return allowAllocation(indexMetadata, allocation);
    }

    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return allowAllocation(allocation.metadata().getIndexSafe(shardRouting.index()), allocation);
    }

    private Decision allowAllocation(IndexMetadata indexMetadata, RoutingAllocation allocation) {
        if (indexMetadata.getCreationVersion().isLegacyIndexVersion()) {
            if (hasValidLicenseSupplier.getAsBoolean()) {
                return allocation.decision(Decision.YES, NAME, "valid license for archive functionality");
            } else {
                return allocation.decision(Decision.NO, NAME, "invalid license for archive functionality");
            }
        } else {
            return allocation.decision(Decision.YES, NAME, "decider only applicable for indices backed by archive functionality");
        }
    }
}
