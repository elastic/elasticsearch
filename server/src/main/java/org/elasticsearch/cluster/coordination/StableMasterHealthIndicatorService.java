/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.health.ServerHealthComponents.CLUSTER_COORDINATION;

public class StableMasterHealthIndicatorService implements HealthIndicatorService {

    public static final String NAME = "stable_master";

    private final ClusterService clusterService;
    private final DiscoveryModule discoveryModule;
    private final MasterHistoryService masterHistoryService;
    private static final Logger logger = LogManager.getLogger(StableMasterHealthIndicatorService.class);

    public StableMasterHealthIndicatorService(
        ClusterService clusterService,
        DiscoveryModule discoveryModule,
        MasterHistoryService masterHistoryService
    ) {
        this.clusterService = clusterService;
        this.discoveryModule = discoveryModule;
        this.masterHistoryService = masterHistoryService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String component() {
        return CLUSTER_COORDINATION;
    }

    @Override
    public HealthIndicatorResult calculate(boolean includeDetails) {
        HealthStatus stableMasterStatus;
        String summary;
        Collection<HealthIndicatorImpact> impacts = new ArrayList<>();
        MasterHistory localMasterHistory = masterHistoryService.getLocalMasterHistory();
        if (hasSeenMasterInLast30Seconds()) {
            logger.info("Have seen a master in the last 30 seconds");
            Set<DiscoveryNode> mastersInLast30Minutes = localMasterHistory.getDistinctMastersSeen();
            if (mastersInLast30Minutes.size() > 2) {
                logger.info("Have seen " + mastersInLast30Minutes.size() + " masters in the last 30 seconds");
                stableMasterStatus = HealthStatus.YELLOW;
                summary = String.format(Locale.ROOT, "%d nodes have acted as master in the last 30 minutes", mastersInLast30Minutes.size());
            } else if (localMasterHistory.hasSameMasterGoneNullNTimes(3)) {
                DiscoveryNode master = localMasterHistory.getMostRecentNonNullMaster();
                logger.info("One master has gone null 3 or more times recently: " + master);
                try {
                    if (masterThinksItIsUnstable(master)) {
                        logger.info("The master node " + master + " thinks it is unstable");
                        stableMasterStatus = HealthStatus.YELLOW;
                        summary = String.format(Locale.ROOT, "Master %s has gone null multiple times in the last 30 minutes", master);
                        impacts.add(new HealthIndicatorImpact(3, "Cluster is at risk of becoming unstable"));
                    } else {
                        logger.info("This node thinks the master is unstable, but the master node " + master + " thinks it is stable");
                        stableMasterStatus = HealthStatus.GREEN;
                        summary = "The cluster has had a stable master node";
                    }
                } catch (ExecutionException e) {
                    logger.error("Exception trying to reach the master", e);
                    stableMasterStatus = HealthStatus.YELLOW;
                    summary = "The cluster has had a master node recently, but erred while attempting to find out if the master had been "
                        + "stable";
                    impacts.add(new HealthIndicatorImpact(3, "Cluster is at risk of becoming unstable"));
                } catch (InterruptedException e) {
                    logger.error("Interrupted while trying to reach the master", e);
                    throw new RuntimeException(e);
                }
            } else {
                logger.info("The cluster has had a stable master node");
                stableMasterStatus = HealthStatus.GREEN;
                summary = "The cluster has had a stable master node";
            }
        } else {
            Collection<DiscoveryNode> masterEligibleNodes = getMasterEligibleNodes();
            if (clusterService.localNode().isMasterNode()) { // if this node is master eligible
                // TODO: reach out to all master-eligible nodes to see if they have discovered each other, and if there's a quorum
                stableMasterStatus = HealthStatus.RED;
                summary = "Something is very wrong";
            } else if (masterEligibleNodes.isEmpty() == false) {
                for (DiscoveryNode node : masterEligibleNodes) {
                    // TODO: Find out if one of these is elected
                }
                stableMasterStatus = HealthStatus.RED;
                summary = "Something is very wrong";
            } else {
                stableMasterStatus = HealthStatus.RED;
                summary = "No master eligible nodes found in the cluster";
                impacts.add(
                    new HealthIndicatorImpact(1, "The cluster cannot create, delete, or rebalance indices, or" + " change settings")
                );
            }
        }

        return createIndicator(stableMasterStatus, summary, includeDetails ? (builder, params) -> {
            builder.startObject();
            return builder.endObject();
        } : HealthIndicatorDetails.EMPTY, impacts);
    }

    private boolean masterThinksItIsUnstable(DiscoveryNode master) throws ExecutionException, InterruptedException {
        return masterHistoryService.getRemoteMasterHistory(master).hasSameMasterGoneNullNTimes(3);
    }

    private boolean hasSeenMasterInLast30Seconds() {
        if (clusterService.state().nodes().getMasterNode() != null) {
            return true;
        }
        return masterHistoryService.getLocalMasterHistory().hasSeenMasterInLastNSeconds(30);
    }

    private Collection<DiscoveryNode> getMasterEligibleNodes() {
        Set<DiscoveryNode> masterEligibleNodes = new HashSet<>();
        discoveryModule.getCoordinator().getFoundPeers().forEach(node -> {
            if (node.isMasterNode()) {
                masterEligibleNodes.add(node);
            }
        });
        return masterEligibleNodes;
    }
}
