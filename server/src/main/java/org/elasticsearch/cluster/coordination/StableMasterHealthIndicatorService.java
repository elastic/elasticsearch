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
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.health.ServerHealthComponents.CLUSTER_COORDINATION;

public class StableMasterHealthIndicatorService implements HealthIndicatorService, ClusterStateListener {

    public static final String NAME = "stable_master";

    private final ClusterService clusterService;
    private List<TimeAndMaster> masterHistory = new ArrayList<>();

    public StableMasterHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
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
        removeOldMasterHistory(System.currentTimeMillis(), true);
        final HealthStatus stableMasterStatus;
        final String summary;
        Collection<HealthIndicatorImpact> impacts = new ArrayList<>();
        if (masterHistory.isEmpty() || masterHistory.get(masterHistory.size() - 1).master == null) {
            stableMasterStatus = HealthStatus.RED;
            summary = "No master";
            impacts.add(new HealthIndicatorImpact(1,"Cannot create, delete, or rebalance indices, or change settings"));
        } else {
            Set<DiscoveryNode> recentMasters = masterHistory.stream().map(TimeAndMaster::master).collect(Collectors.toSet());
            if (recentMasters.size() > 2) {
                stableMasterStatus = HealthStatus.RED;
                summary = String.format(Locale.ROOT, "The cluster has had %s master nodes in the last 30 minutes: %s",
                    recentMasters.size(), recentMasters);
                impacts.add(new HealthIndicatorImpact(2,"The cluster is at risk of failing to create, delete, or rebalance indices, or to" +
                    " change settings"));
            } else {
                stableMasterStatus = HealthStatus.GREEN;
                summary = "The cluster has had a stable master node";
            }
        }

        return createIndicator(stableMasterStatus, summary, includeDetails ? (builder, params) -> {
            builder.startObject();
            return builder.endObject();
        } : HealthIndicatorDetails.EMPTY, impacts);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        long now = System.currentTimeMillis();
        DiscoveryNode currentMaster = event.state().nodes().getMasterNode();
        DiscoveryNode previousMaster = event.previousState().nodes().getMasterNode();
        removeOldMasterHistory(now, false);
        if (currentMaster == null || currentMaster.equals(previousMaster) == false || masterHistory.isEmpty()) {
            masterHistory.add(new TimeAndMaster(now, currentMaster));
        }
    }

    /**
     * Clears out anything from masterHistory that is from more than 30 minutes before now. If leaveNewestEvenIfOld is true, the newest
     * entry in masterHistory will reamin in masterHistory even if it is from more than 30 minutes before now.
     * @param now The current time
     * @param leaveNewestEvenIfOld If true, the most recent entry will not be removed from watcherHistory even if it is older than 30
     *                             minutes
     */
    private void removeOldMasterHistory(long now, boolean leaveNewestEvenIfOld) {
        if (leaveNewestEvenIfOld && masterHistory.size() < 2) {
            return;
        }
        long thirtyMinutesAgo = now - (30 * 60 * 1000);
        TimeAndMaster mostRecent = masterHistory.isEmpty() ? null : masterHistory.get(masterHistory.size() - 1);
        masterHistory = masterHistory.stream().filter(timeAndMaster -> timeAndMaster.time > thirtyMinutesAgo).collect(Collectors.toList());
        if (masterHistory.isEmpty() && leaveNewestEvenIfOld) { // The most recent entry was more than 30 minutes ago
            masterHistory.add(mostRecent);
        }
    }

    private static record TimeAndMaster(long time, DiscoveryNode master) {
    }
}
