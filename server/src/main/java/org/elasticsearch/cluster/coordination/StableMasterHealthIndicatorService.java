/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
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
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.health.ServerHealthComponents.CLUSTER_COORDINATION;

public class StableMasterHealthIndicatorService implements HealthIndicatorService, ClusterStateListener {

    public static final String NAME = "stable_master";

    private final ClusterService clusterService;
    private final DiscoveryModule discoveryModule;
    private List<TimeAndMaster> masterHistory = new ArrayList<>();

    Supplier<Long> nowSupplier = System::currentTimeMillis; // Can be changed for testing

    public StableMasterHealthIndicatorService(ClusterService clusterService, DiscoveryModule discoveryModule) {
        this.clusterService = clusterService;
        this.discoveryModule = discoveryModule;
        clusterService.addListener(this);
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
        removeOldMasterHistory(true);
        HealthStatus stableMasterStatus;
        String summary;
        Collection<HealthIndicatorImpact> impacts = new ArrayList<>();
        if (hasSeenMasterInLast30Seconds()) {
            Set<DiscoveryNode> mastersInLast30Minutes = getDistinctMastersIn30Minutes();
            if (mastersInLast30Minutes.size() > 2) {
                stableMasterStatus = HealthStatus.YELLOW;
                summary = String.format(Locale.ROOT, "%d nodes have acted as master in the last 30 minutes", mastersInLast30Minutes.size());
            } else if (hasMasterGoneNullThreeTimesIn30Mintutes()) {
                List<TimeAndMaster> nonNullMasters = masterHistory.stream()
                    .filter(timeAndMaster -> timeAndMaster.master != null)
                    .collect(Collectors.toList());
                TimeAndMaster timeAndMaster = nonNullMasters.get(nonNullMasters.size() - 1);
                try {
                    if (masterThinksItIsUnstable(timeAndMaster.master)) {
                        stableMasterStatus = HealthStatus.YELLOW;
                        summary = String.format(
                            Locale.ROOT,
                            "Master %s has gone null multiple times in the last 30 minutes",
                            timeAndMaster.master.toString()
                        );
                        impacts.add(new HealthIndicatorImpact(3, "Cluster is at risk of becoming unstable"));
                    } else {
                        stableMasterStatus = HealthStatus.GREEN;
                        summary = "The cluster has had a stable master node";
                    }
                } catch (TimeoutException e) {
                    stableMasterStatus = HealthStatus.YELLOW;
                    summary = "The cluster has had a master node recently, but timed out attempting to find out if the master had been "
                        + "stable";
                    impacts.add(new HealthIndicatorImpact(3, "Cluster is at risk of becoming unstable"));
                }
            } else {
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

    private boolean masterThinksItIsUnstable(DiscoveryNode master) throws TimeoutException {
        return false; // TODO: reach out to master
    }

    private boolean hasMasterGoneNullThreeTimesIn30Mintutes() {
        return masterHistory.stream().filter(timeAndMaster -> timeAndMaster.master == null).count() > 2
            && getDistinctMastersIn30Minutes().size() == 1;
    }

    private Set<DiscoveryNode> getDistinctMastersIn30Minutes() {
        return masterHistory.stream()
            .filter(timeAndMaster -> timeAndMaster.master != null)
            .map(TimeAndMaster::master)
            .collect(Collectors.toSet());
    }

    private boolean hasSeenMasterInLast30Seconds() {
        if (clusterService.state().nodes().getMasterNode() != null) {
            return true;
        }
        long now = nowSupplier.get();
        long thirtySecondsAgo = now - (30 * 1000);
        return getCurrentMaster() != null
            || masterHistory.stream().anyMatch(timeAndMaster -> timeAndMaster.time > thirtySecondsAgo && timeAndMaster.master != null);
    }

    private DiscoveryNode getCurrentMaster() {
        return masterHistory.isEmpty() ? null : masterHistory.get(masterHistory.size() - 1).master;
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

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode currentMaster = event.state().nodes().getMasterNode();
        DiscoveryNode previousMaster = event.previousState().nodes().getMasterNode();
        removeOldMasterHistory(false);
        if (currentMaster == null || currentMaster.equals(previousMaster) == false || masterHistory.isEmpty()) {
            masterHistory.add(new TimeAndMaster(nowSupplier.get(), currentMaster));
        }
    }

    /**
     * Clears out anything from masterHistory that is from more than 30 minutes before now. If leaveNewestEvenIfOld is true, the newest
     * entry in masterHistory will reamin in masterHistory even if it is from more than 30 minutes before now.
     * @param leaveNewestEvenIfOld If true, the most recent entry will not be removed from watcherHistory even if it is older than 30
     *                             minutes
     */
    private void removeOldMasterHistory(boolean leaveNewestEvenIfOld) {
        if (leaveNewestEvenIfOld && masterHistory.size() < 2) {
            return;
        }
        long now = nowSupplier.get();
        long thirtyMinutesAgo = now - (30 * 60 * 1000);
        TimeAndMaster mostRecent = masterHistory.isEmpty() ? null : masterHistory.get(masterHistory.size() - 1);
        masterHistory = masterHistory.stream().filter(timeAndMaster -> timeAndMaster.time > thirtyMinutesAgo).collect(Collectors.toList());
        if (masterHistory.isEmpty() && leaveNewestEvenIfOld) { // The most recent entry was more than 30 minutes ago
            masterHistory.add(mostRecent);
        }
    }

    private static record TimeAndMaster(long time, DiscoveryNode master) {}
}
