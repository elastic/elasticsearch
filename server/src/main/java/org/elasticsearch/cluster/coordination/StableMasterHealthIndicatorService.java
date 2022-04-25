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
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
        Map<String, Object> details = new HashMap<>();
        Collection<HealthIndicatorImpact> impacts = new ArrayList<>();
        MasterHistory localMasterHistory = masterHistoryService.getLocalMasterHistory();
        if (hasSeenMasterInLast30Seconds()) {
            logger.trace("Have seen a master in the last 30 seconds");
            if (localMasterHistory.hasSameMasterGoneNullNTimes(3)) {
                DiscoveryNode master = localMasterHistory.getMostRecentNonNullMaster();
                logger.trace("One master has gone null 3 or more times recently: " + master);
                List<DiscoveryNode> remoteHistory = masterHistoryService.getRemoteMasterHistory(master);
                if (remoteHistory == null || MasterHistory.hasSameMasterGoneNullNTimes(remoteHistory, 3)) {
                    if (remoteHistory == null) {
                        logger.trace(String.format(Locale.ROOT, "Unable to get master history from %s", master));
                    } else {
                        logger.trace(String.format(Locale.ROOT, "The master node %s thinks it is unstable", master));
                    }
                    stableMasterStatus = HealthStatus.YELLOW;
                    summary = String.format(
                        Locale.ROOT,
                        "The cluster's master has alternated between %s and no master multiple times in the last 30 minutes",
                        master
                    );
                    impacts.add(
                        new HealthIndicatorImpact(
                            3,
                            "The cluster is at risk of not being able to create, delete, or rebalance indices",
                            List.of(ImpactArea.INGEST)
                        )
                    );
                    if (includeDetails) {
                        details.put("current_master", localMasterHistory.getCurrentMaster());
                    }
                } else {
                    logger.trace(
                        String.format(
                            Locale.ROOT,
                            "This node thinks the master is unstable, but the master node %s thinks it is stable",
                            master
                        )
                    );
                    stableMasterStatus = HealthStatus.GREEN;
                    summary = "The cluster has a stable master node";
                }
            } else if (localMasterHistory.getDistinctMastersSeen().size() > 1 && localMasterHistory.getImmutableView().size() > 3) {
                List<DiscoveryNode> mastersInLast30Minutes = localMasterHistory.getImmutableView();
                logger.trace("Have seen " + (mastersInLast30Minutes.size() - 1) + " master changes in the last 30 seconds");
                stableMasterStatus = HealthStatus.YELLOW;
                summary = String.format(
                    Locale.ROOT,
                    "The master has changed %d times in the last 30 minutes",
                    mastersInLast30Minutes.size() - 1
                );
                impacts.add(
                    new HealthIndicatorImpact(
                        3,
                        "The cluster currently has a master node, but having multiple master node changes in a short time is an indicator "
                            + "that the cluster is at risk of of not being able to create, delete, or rebalance indices",
                        List.of(ImpactArea.INGEST)
                    )
                );
                if (includeDetails) {
                    details.put("current_master", localMasterHistory.getCurrentMaster());
                    details.put("recent_masters", mastersInLast30Minutes);
                }
            } else {
                logger.trace("The cluster has a stable master node");
                stableMasterStatus = HealthStatus.GREEN;
                summary = "The cluster has a stable master node";
            }
        } else {
            stableMasterStatus = HealthStatus.GREEN;
            summary = "Placeholder summary";
        }

        return createIndicator(
            stableMasterStatus,
            summary,
            includeDetails ? new SimpleHealthIndicatorDetails(details) : HealthIndicatorDetails.EMPTY,
            impacts
        );
    }

    private boolean masterThinksItIsUnstable(DiscoveryNode master) throws ExecutionException, InterruptedException {
        logger.trace(String.format(Locale.ROOT, "Reaching out to %s to see if it thinks it has been unstable", master));
        List<DiscoveryNode> remoteHistory = masterHistoryService.getRemoteMasterHistory(master);
        return MasterHistory.hasSameMasterGoneNullNTimes(remoteHistory, 3);
    }

    private boolean hasSeenMasterInLast30Seconds() {
        if (clusterService.state().nodes().getMasterNode() != null) {
            return true;
        }
        return masterHistoryService.getLocalMasterHistory().hasSeenMasterInLastNSeconds(30);
    }
}
