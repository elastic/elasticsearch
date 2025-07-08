/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.ilm.step.info.AllocationInfo;

import java.util.List;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ilm.AllocationRoutedStep.getPendingAllocations;
import static org.elasticsearch.xpack.core.ilm.step.info.AllocationInfo.waitingForActiveShardsAllocationInfo;

/**
 * Checks whether all shards have been correctly routed in response to updating the allocation rules for an index in order
 * to migrate the index to a new tier.
 */
public class DataTierMigrationRoutedStep extends ClusterStateWaitStep {
    public static final String NAME = "check-migration";

    private static final Logger logger = LogManager.getLogger(DataTierMigrationRoutedStep.class);

    private static final AllocationDeciders DECIDERS = new AllocationDeciders(List.of(DataTierAllocationDecider.INSTANCE));

    DataTierMigrationRoutedStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public Result isConditionMet(Index index, ProjectState currentState) {
        IndexMetadata idxMeta = currentState.metadata().index(index);
        if (idxMeta == null) {
            // Index must have been since deleted, ignore it
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().action(), index.getName());
            return new Result(false, null);
        }
        List<String> preferredTierConfiguration = idxMeta.getTierPreference();
        Optional<String> availableDestinationTier = DataTierAllocationDecider.preferredAvailableTier(
            preferredTierConfiguration,
            currentState.cluster().getNodes(),
            DesiredNodes.latestFromClusterState(currentState.cluster()),
            currentState.cluster().metadata().nodeShutdowns()
        );

        if (ActiveShardCount.ALL.enoughShardsActive(currentState.metadata(), currentState.routingTable(), index.getName()) == false) {
            if (preferredTierConfiguration.isEmpty()) {
                logger.debug(
                    "[{}] lifecycle action for index [{}] cannot make progress because not all shards are active",
                    getKey().action(),
                    index.getName()
                );
            } else {
                if (availableDestinationTier.isPresent()) {
                    logger.debug(
                        "[{}] migration of index [{}] to the {} tier preference cannot progress, as not all shards are active",
                        getKey().action(),
                        index.getName(),
                        preferredTierConfiguration
                    );
                } else {
                    logger.debug(
                        "[{}] migration of index [{}] to the next tier cannot progress as there is no available tier for the "
                            + "configured preferred tiers {} and not all shards are active",
                        getKey().action(),
                        index.getName(),
                        preferredTierConfiguration
                    );
                }
            }
            return new Result(false, waitingForActiveShardsAllocationInfo(idxMeta.getNumberOfReplicas()));
        }

        if (preferredTierConfiguration.isEmpty()) {
            logger.debug(
                "index [{}] has no data tier routing preference setting configured and all its shards are active. considering "
                    + "the [{}] step condition met and continuing to the next step",
                index.getName(),
                getKey().name()
            );
            // the user removed the tier routing setting and all the shards are active so we'll cary on
            return new Result(true, null);
        }

        int allocationPendingAllShards = getPendingAllocations(index, DECIDERS, currentState);

        if (allocationPendingAllShards > 0) {
            String statusMessage = availableDestinationTier.map(
                s -> Strings.format(
                    "[%s] lifecycle action [%s] waiting for [%s] shards to be moved to the [%s] tier (tier "
                        + "migration preference configuration is %s)",
                    index.getName(),
                    getKey().action(),
                    allocationPendingAllShards,
                    s,
                    preferredTierConfiguration
                )
            )
                .orElseGet(
                    () -> Strings.format(
                        "index [%s] has a preference for tiers %s, but no nodes for any of those tiers are available in the cluster",
                        index.getName(),
                        preferredTierConfiguration
                    )
                );
            logger.debug(statusMessage);
            return new Result(false, new AllocationInfo(idxMeta.getNumberOfReplicas(), allocationPendingAllShards, true, statusMessage));
        } else {
            logger.debug(
                "[{}] migration of index [{}] to tier [{}] (preference [{}]) complete",
                getKey().action(),
                index,
                availableDestinationTier.orElse(""),
                preferredTierConfiguration
            );
            return new Result(true, null);
        }
    }
}
