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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.UserAction;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.health.ServerHealthComponents.CLUSTER_COORDINATION;

/**
 * This indicator reports the health of master stability.
 * If we have had a master within the last 30 seconds, and that master has not changed more than 3 times in the last 30 minutes, then
 * this will report GREEN.
 * If we have had a master within the last 30 seconds, but that master has changed more than 3 times in the last 30 minutes (and that is
 * confirmed by checking with the last-known master), then this will report YELLOW.
 * If we have not had a master within the last 30 seconds, then this will will report RED with one exception. That exception is when:
 * (1) no node is elected master, (2) this node is not master eligible, (3) some node is master eligible, (4) we ask a master-eligible node
 * to run this indicator, and (5) it comes back with a result that is not RED.
 * Since this indicator needs to be able to run when there is no master at all, it does not depend on the dedicated health node (which
 * requires the existence of a master).
 */
public class StableMasterHealthIndicatorService implements HealthIndicatorService, ClusterStateListener {

    public static final String NAME = "stable_master";

    private final ClusterService clusterService;
    private final MasterHistoryService masterHistoryService;
    private static final Logger logger = LogManager.getLogger(StableMasterHealthIndicatorService.class);
    /**
     * This is the amount of time we use to make the initial decision -- have we seen a master node in the very recent past?
     */
    private static final TimeValue VERY_RECENT_PAST = new TimeValue(30, TimeUnit.SECONDS);

    /**
     * This is the number of times that it is OK for the master history to show a transition from a non-null master to a null master before
     * it starts impacting the health status.
     */
    private static final int ACCEPTABLE_NULL_TRANSITIONS = 3;
    /**
     * This is the number of times that it is OK for the master history to show a transition one non-null master to a different non-null
     * master before it starts impacting the health status.
     */
    private static final int ACCEPTABLE_IDENTITY_CHANGES = 3;
    /**
     * The severity to use for a low-severity impact in this indicator
     */
    private static final int LOW_IMPACT_SEVERITY = 3;

    public StableMasterHealthIndicatorService(ClusterService clusterService, MasterHistoryService masterHistoryService) {
        this.clusterService = clusterService;
        this.masterHistoryService = masterHistoryService;
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
        MasterHistory localMasterHistory = masterHistoryService.getLocalMasterHistory();
        if (hasSeenMasterInVeryRecentPast()) {
            return calculateWhenHaveSeenMasterRecently(localMasterHistory, includeDetails);
        } else {
            return calculateWhenHaveNotSeenMasterRecently(localMasterHistory, includeDetails);
        }
    }

    /**
     * Returns the health result for the case when we have seen a master recently (at some point in the last 30 seconds).
     * @param localMasterHistory The master history as seen from the local machine
     * @param includeDetails Whether to calculate and include the details in the result
     * @return The HealthIndicatorResult for the given localMasterHistory
     */
    private HealthIndicatorResult calculateWhenHaveSeenMasterRecently(MasterHistory localMasterHistory, boolean includeDetails) {
        int masterChanges = MasterHistory.getNumberOfMasterIdentityChanges(localMasterHistory.getNodes());
        logger.trace("Have seen a master in the last {}): {}", VERY_RECENT_PAST, localMasterHistory.getMostRecentNonNullMaster());
        final HealthIndicatorResult result;
        if (masterChanges > ACCEPTABLE_IDENTITY_CHANGES) {
            result = calculateWhenMasterHasChangedIdentity(localMasterHistory, masterChanges, includeDetails);
        } else if (localMasterHistory.hasMasterGoneNullAtLeastNTimes(ACCEPTABLE_NULL_TRANSITIONS + 1)) {
            result = calculateWhenMasterHasFlappedNull(localMasterHistory, includeDetails);
        } else {
            result = getMasterIsStableResult();
        }
        return result;
    }

    /**
     * Returns the health result when we have detected locally that the master has changed identity repeatedly (more than 3 times in the
     * last 30 minutes)
     * @param localMasterHistory The master history as seen from the local machine
     * @param masterChanges The number of times that the local machine has seen the master identity change in the last 30 minutes
     * @param includeDetails Whether to calculate and include the details in the result
     * @return The HealthIndicatorResult for the given localMasterHistory
     */
    private HealthIndicatorResult calculateWhenMasterHasChangedIdentity(
        MasterHistory localMasterHistory,
        int masterChanges,
        boolean includeDetails
    ) {
        logger.trace("Have seen {} master changes in the last {}}", masterChanges, MasterHistory.MAX_HISTORY_AGE);
        HealthStatus stableMasterStatus = HealthStatus.YELLOW;
        String summary = String.format(
            Locale.ROOT,
            "The master has changed %d times in the last %s",
            masterChanges,
            MasterHistory.MAX_HISTORY_AGE
        );
        Map<String, Object> details = new HashMap<>();
        Collection<HealthIndicatorImpact> impacts = new ArrayList<>();
        List<UserAction> userActions = new ArrayList<>();
        impacts.add(
            new HealthIndicatorImpact(
                LOW_IMPACT_SEVERITY,
                "The cluster currently has a master node, but having multiple master node changes in a short time is an indicator "
                    + "that the cluster is at risk of of not being able to create, delete, or rebalance indices",
                List.of(ImpactArea.INGEST)
            )
        );
        if (includeDetails) {
            List<DiscoveryNode> recentMasters = localMasterHistory.getNodes();
            details.put("current_master", new DiscoveryNodeXContentObject(localMasterHistory.getMostRecentMaster()));
            details.put("recent_masters", recentMasters.stream().map(DiscoveryNodeXContentObject::new).toList());
        }
        return createIndicator(
            stableMasterStatus,
            summary,
            includeDetails ? new SimpleHealthIndicatorDetails(details) : HealthIndicatorDetails.EMPTY,
            impacts,
            userActions
        );
    }

    /**
     * Returns the health result when we have detected locally that the master has changed to null repeatedly (more than 3 times in the last
     * 30 minutes)
     * @param localMasterHistory The master history as seen from the local machine
     * @param includeDetails Whether to calculate and include the details in the result
     * @return The HealthIndicatorResult for the given localMasterHistory
     */
    private HealthIndicatorResult calculateWhenMasterHasFlappedNull(MasterHistory localMasterHistory, boolean includeDetails) {
        HealthStatus stableMasterStatus;
        String summary;
        Map<String, Object> details = new HashMap<>();
        Collection<HealthIndicatorImpact> impacts = new ArrayList<>();
        List<UserAction> userActions = new ArrayList<>();
        DiscoveryNode master = localMasterHistory.getMostRecentNonNullMaster();
        logger.trace("One master has gone null {} or more times recently: {}", ACCEPTABLE_NULL_TRANSITIONS + 1, master);
        boolean localNodeIsMaster = clusterService.localNode().equals(master);
        final List<DiscoveryNode> remoteHistory;
        if (localNodeIsMaster) {
            remoteHistory = null; // We don't need to fetch the remote master's history if we are that remote master
        } else {
            remoteHistory = masterHistoryService.getRemoteMasterHistory(master);
        }
        if (localNodeIsMaster
            || remoteHistory == null
            || MasterHistory.hasMasterGoneNullAtLeastNTimes(remoteHistory, ACCEPTABLE_NULL_TRANSITIONS + 1)
            || MasterHistory.getNumberOfMasterIdentityChanges(remoteHistory) > ACCEPTABLE_IDENTITY_CHANGES) {
            if (localNodeIsMaster == false && remoteHistory == null) {
                logger.trace("Unable to get master history from {}}", master);
            } else {
                logger.trace("The master node {} thinks it is unstable", master);
            }
            stableMasterStatus = HealthStatus.YELLOW;
            summary = String.format(
                Locale.ROOT,
                "The cluster's master has alternated between %s and no master multiple times in the last %s",
                master,
                MasterHistory.MAX_HISTORY_AGE
            );
            impacts.add(
                new HealthIndicatorImpact(
                    LOW_IMPACT_SEVERITY,
                    "The cluster is at risk of not being able to create, delete, or rebalance indices",
                    List.of(ImpactArea.INGEST)
                )
            );
            if (includeDetails) {
                details.put("current_master", new DiscoveryNodeXContentObject(localMasterHistory.getMostRecentMaster()));
            }
        } else {
            logger.trace("This node thinks the master is unstable, but the master node {} thinks it is stable", master);
            stableMasterStatus = HealthStatus.GREEN;
            summary = "The cluster has a stable master node";
        }
        return createIndicator(
            stableMasterStatus,
            summary,
            includeDetails ? new SimpleHealthIndicatorDetails(details) : HealthIndicatorDetails.EMPTY,
            impacts,
            userActions
        );
    }

    /**
     * Returns a HealthIndicatorResult for the case when the master is seen as stable
     * @return A HealthIndicatorResult for the case when the master is seen as stable (GREEN status, no impacts or details)
     */
    private HealthIndicatorResult getMasterIsStableResult() {
        HealthStatus stableMasterStatus = HealthStatus.GREEN;
        String summary = "The cluster has a stable master node";
        Collection<HealthIndicatorImpact> impacts = new ArrayList<>();
        List<UserAction> userActions = new ArrayList<>();
        logger.trace("The cluster has a stable master node");
        return createIndicator(stableMasterStatus, summary, HealthIndicatorDetails.EMPTY, impacts, userActions);
    }

    /**
     * Returns the health result for the case when we have NOT seen a master recently (at some point in the last 30 seconds).
     * @param localMasterHistory The master history as seen from the local machine
     * @param includeDetails Whether to calculate and include the details in the result
     * @return The HealthIndicatorResult for the given localMasterHistory
     */
    private HealthIndicatorResult calculateWhenHaveNotSeenMasterRecently(MasterHistory localMasterHistory, boolean includeDetails) {
        // NOTE: The logic in this method will be implemented in a future PR
        HealthStatus stableMasterStatus = HealthStatus.RED;
        String summary = "Placeholder summary";
        Map<String, Object> details = new HashMap<>();
        Collection<HealthIndicatorImpact> impacts = new ArrayList<>();
        List<UserAction> userActions = new ArrayList<>();
        return createIndicator(
            stableMasterStatus,
            summary,
            includeDetails ? new SimpleHealthIndicatorDetails(details) : HealthIndicatorDetails.EMPTY,
            impacts,
            userActions
        );
    }

    private boolean hasSeenMasterInVeryRecentPast() {
        if (clusterService.state().nodes().getMasterNode() != null) {
            return true;
        }
        return masterHistoryService.getLocalMasterHistory().hasSeenMasterInLastNSeconds((int) VERY_RECENT_PAST.seconds());
    }

    /*
     * If we detect that the same master has gone null 3 or more times, we ask the MasterHistoryService to fetch the master history
     * as seen from that node so that it is ready in case a health API request comes in.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode currentMaster = event.state().nodes().getMasterNode();
        DiscoveryNode previousMaster = event.previousState().nodes().getMasterNode();
        if (currentMaster == null && previousMaster != null) {
            if (masterHistoryService.getLocalMasterHistory().hasMasterGoneNullAtLeastNTimes(ACCEPTABLE_NULL_TRANSITIONS + 1)) {
                DiscoveryNode master = masterHistoryService.getLocalMasterHistory().getMostRecentNonNullMaster();
                if (master != null && clusterService.localNode().equals(master) == false) {
                    masterHistoryService.requestRemoteMasterHistory(master);
                }
            }
        }
    }

    /**
     * XContentBuilder doesn't deal well with ToXContentFragments (which is what DiscoveryNodes are). Also XContentBuilder doesn't do well
     * with null values in lists. This object wraps the DiscoveryNode's XContent in a start and end object, and writes out nulls as empty
     * objects.
     */
    private record DiscoveryNodeXContentObject(DiscoveryNode master) implements ToXContentObject {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (master != null) {
                master.toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }
    }
}
