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
import org.elasticsearch.common.settings.Setting;
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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    /**
     * This is the amount of time we use to make the initial decision -- have we seen a master node in the very recent past?
     */
    private final TimeValue veryRecentPast;
    /**
     * This is the number of times that it is OK for the master history to show a transition from a non-null master to a null master before
     * it starts impacting the health status.
     */
    private final int acceptableNullTransitions;
    /**
     * This is the number of times that it is OK for the master history to show a transition one non-null master to a different non-null
     * master before it starts impacting the health status.
     */
    private final int acceptableIdentityChanges;

    private static final Logger logger = LogManager.getLogger(StableMasterHealthIndicatorService.class);

    private static final TimeValue DEFAULT_VERY_RECENT_PAST = new TimeValue(30, TimeUnit.SECONDS);
    private static final TimeValue SMALLEST_ALLOWED_VERY_RECENT_PAST = new TimeValue(1, TimeUnit.SECONDS);

    private static final int DEFAULT_ACCEPTABLE_NULL_TRANSITIONS = 3;
    private static final int SMALLEST_ALLOWED_ACCEPTABLE_NULL_TRANSITIONS = 0;

    private static final int DEFAULT_ACCEPTABLE_IDENTITY_CHANGES = 3;
    private static final int SMALLEST_ALLOWED_ACCEPTABLE_IDENTITY_CHANGES = 0;

    // Keys for the details map:
    private static final String DETAILS_CURRENT_MASTER = "current_master";
    private static final String DETAILS_RECENT_MASTERS = "recent_masters";
    private static final String DETAILS_EXCEPTION_FETCHING_HISTORY = "exception_fetching_history";
    private static final String DETAILS_EXCEPTION_FETCHING_HISTORY_STACK_TRACE = "exception_fetching_history_stack_trace";

    // Impacts of having an unstable master:
    private static final String UNSTABLE_MASTER_INGEST_IMPACT = "The cluster cannot create, delete, or rebalance indices, and cannot "
        + "insert or update documents.";
    private static final String UNSTABLE_MASTER_DEPLOYMENT_MANAGEMENT_IMPACT = "Scheduled tasks such as Watcher, ILM, and SLM will not "
        + "work. The _cat APIs will not work.";
    private static final String UNSTABLE_MASTER_BACKUP_IMPACT = "Snapshot and restore will not work.";

    /**
     * The severity to use for a low-severity impact in this indicator
     */
    private static final int LOW_IMPACT_SEVERITY = 3;

    public static final Setting<TimeValue> VERY_RECENT_PAST_SETTING = Setting.timeSetting(
        "health.master_history.very_recent_past",
        DEFAULT_VERY_RECENT_PAST,
        SMALLEST_ALLOWED_VERY_RECENT_PAST,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> ACCEPTABLE_NULL_TRANSITIONS_SETTING = Setting.intSetting(
        "health.master_history.acceptable_null_transitions",
        DEFAULT_ACCEPTABLE_NULL_TRANSITIONS,
        SMALLEST_ALLOWED_ACCEPTABLE_NULL_TRANSITIONS,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> ACCEPTABLE_IDENTITY_CHANGES_SETTING = Setting.intSetting(
        "health.master_history.acceptable_identity_changes",
        DEFAULT_ACCEPTABLE_IDENTITY_CHANGES,
        SMALLEST_ALLOWED_ACCEPTABLE_IDENTITY_CHANGES,
        Setting.Property.NodeScope
    );

    public StableMasterHealthIndicatorService(ClusterService clusterService, MasterHistoryService masterHistoryService) {
        this.clusterService = clusterService;
        this.masterHistoryService = masterHistoryService;
        this.veryRecentPast = VERY_RECENT_PAST_SETTING.get(clusterService.getSettings());
        this.acceptableNullTransitions = ACCEPTABLE_NULL_TRANSITIONS_SETTING.get(clusterService.getSettings());
        this.acceptableIdentityChanges = ACCEPTABLE_IDENTITY_CHANGES_SETTING.get(clusterService.getSettings());
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
    public HealthIndicatorResult calculate(boolean explain) {
        MasterHistory localMasterHistory = masterHistoryService.getLocalMasterHistory();
        if (hasSeenMasterInVeryRecentPast()) {
            return calculateWhenHaveSeenMasterRecently(localMasterHistory, explain);
        } else {
            return calculateWhenHaveNotSeenMasterRecently(localMasterHistory, explain);
        }
    }

    /**
     * Returns the health result for the case when we have seen a master recently (at some point in the last 30 seconds).
     * @param localMasterHistory The master history as seen from the local machine
     * @param explain Whether to calculate and include the details in the result
     * @return The HealthIndicatorResult for the given localMasterHistory
     */
    private HealthIndicatorResult calculateWhenHaveSeenMasterRecently(MasterHistory localMasterHistory, boolean explain) {
        int masterChanges = MasterHistory.getNumberOfMasterIdentityChanges(localMasterHistory.getNodes());
        logger.trace("Have seen a master in the last {}): {}", veryRecentPast, localMasterHistory.getMostRecentNonNullMaster());
        final HealthIndicatorResult result;
        if (masterChanges > acceptableIdentityChanges) {
            result = calculateWhenMasterHasChangedIdentity(localMasterHistory, masterChanges, explain);
        } else if (localMasterHistory.hasMasterGoneNullAtLeastNTimes(acceptableNullTransitions + 1)) {
            result = calculateWhenMasterHasFlappedNull(localMasterHistory, explain);
        } else {
            result = getMasterIsStableResult(explain, localMasterHistory);
        }
        return result;
    }

    /**
     * Returns the health result when we have detected locally that the master has changed identity repeatedly (more than 3 times in the
     * last 30 minutes)
     * @param localMasterHistory The master history as seen from the local machine
     * @param masterChanges The number of times that the local machine has seen the master identity change in the last 30 minutes
     * @param explain Whether to calculate and include the details in the result
     * @return The HealthIndicatorResult for the given localMasterHistory
     */
    private HealthIndicatorResult calculateWhenMasterHasChangedIdentity(
        MasterHistory localMasterHistory,
        int masterChanges,
        boolean explain
    ) {
        logger.trace("Have seen {} master changes in the last {}}", masterChanges, localMasterHistory.getMaxHistoryAge());
        HealthStatus stableMasterStatus = HealthStatus.YELLOW;
        String summary = String.format(
            Locale.ROOT,
            "The master has changed %d times in the last %s",
            masterChanges,
            localMasterHistory.getMaxHistoryAge()
        );
        Map<String, Object> details = getSimpleDetailsMap(explain, localMasterHistory);
        Collection<HealthIndicatorImpact> impacts = getUnstableMasterImpacts();
        List<UserAction> userActions = getContactSupportUserActions(explain);
        return createIndicator(
            stableMasterStatus,
            summary,
            explain ? new SimpleHealthIndicatorDetails(details) : HealthIndicatorDetails.EMPTY,
            impacts,
            userActions
        );
    }

    /**
     * This returns an empty map if explain is false, otherwise a map of details containing only "current_master" and "recent_masters".
     * @param explain If true, the map will contain "current_master" and "recent_masters". Otherwise it will be empty.
     * @param localMasterHistory The MasterHistory object to pull current and recent master info from
     * @return An empty map if explain is false, otherwise a map of details containing only "current_master" and "recent_masters"
     */
    private Map<String, Object> getSimpleDetailsMap(boolean explain, MasterHistory localMasterHistory) {
        Map<String, Object> details = new HashMap<>();
        if (explain) {
            List<DiscoveryNode> recentMasters = localMasterHistory.getNodes();
            details.put(DETAILS_CURRENT_MASTER, new DiscoveryNodeXContentObject(localMasterHistory.getMostRecentMaster()));
            // Having the nulls in the recent masters xcontent list is not helpful, so we filter them out:
            details.put(
                DETAILS_RECENT_MASTERS,
                recentMasters.stream().filter(Objects::nonNull).map(DiscoveryNodeXContentObject::new).toList()
            );
        }
        return details;
    }

    /**
     * This method returns the only user action that is relevant when the master is unstable -- contact support.
     * @param explain If true, the returned list includes a UserAction to contact support, otherwise an empty list
     * @return a single UserAction instructing users to contact support.
     */
    private List<UserAction> getContactSupportUserActions(boolean explain) {
        if (explain) {
            UserAction.Definition contactSupport = new UserAction.Definition(
                "contact_support",
                "The Elasticsearch cluster does not have a stable master node. This almost always requires expert assistance. Please "
                    + "contact Elastic support to resolve the problem.",
                null
            );
            UserAction userAction = new UserAction(contactSupport, null);
            return List.of(userAction);
        } else {
            return List.of();
        }
    }

    /**
     * Returns the list of the impacts of an unstable master node.
     * @return The list of the impacts of an unstable master node
     */
    private List<HealthIndicatorImpact> getUnstableMasterImpacts() {
        List<HealthIndicatorImpact> impacts = new ArrayList<>();
        impacts.add(new HealthIndicatorImpact(1, UNSTABLE_MASTER_INGEST_IMPACT, List.of(ImpactArea.INGEST)));
        impacts.add(new HealthIndicatorImpact(1, UNSTABLE_MASTER_DEPLOYMENT_MANAGEMENT_IMPACT, List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)));
        impacts.add(new HealthIndicatorImpact(3, UNSTABLE_MASTER_BACKUP_IMPACT, List.of(ImpactArea.BACKUP)));
        return impacts;
    }

    /**
     * Returns the health result when we have detected locally that the master has changed to null repeatedly (more than 3 times in the last
     * 30 minutes). This method attemtps to use the master history from a remote node to confirm what we are seeing locally. If the
     * information from the remote node confirms that the master history has been unstable, a YELLOW status is returned. If the
     * information from the remote node shows that the master history has been stable, then we assume that the problem is with this node
     * and a GREEN status is returned (the problems with this node will be covered in a different health indicator). If there had been
     * problems fetching the remote master history, the exception seen will be included in the details of the result.
     * @param localMasterHistory The master history as seen from the local machine
     * @param explain Whether to calculate and include the details in the result
     * @return The HealthIndicatorResult for the given localMasterHistory
     */
    private HealthIndicatorResult calculateWhenMasterHasFlappedNull(MasterHistory localMasterHistory, boolean explain) {
        final HealthStatus stableMasterStatus;
        final String summary;
        Map<String, Object> details = new HashMap<>();
        final Collection<HealthIndicatorImpact> impacts;
        final List<UserAction> userActions;
        DiscoveryNode master = localMasterHistory.getMostRecentNonNullMaster();
        logger.trace("One master has gone null {} or more times recently: {}", acceptableNullTransitions + 1, master);
        boolean localNodeIsMaster = clusterService.localNode().equals(master);
        List<DiscoveryNode> remoteHistory;
        Exception remoteHistoryException = null;
        if (localNodeIsMaster) {
            remoteHistory = null; // We don't need to fetch the remote master's history if we are that remote master
        } else {
            try {
                remoteHistory = masterHistoryService.getRemoteMasterHistory();
            } catch (Exception e) {
                remoteHistory = null;
                remoteHistoryException = e;
            }
        }
        if (localNodeIsMaster
            || remoteHistory == null
            || MasterHistory.hasMasterGoneNullAtLeastNTimes(remoteHistory, acceptableNullTransitions + 1)
            || MasterHistory.getNumberOfMasterIdentityChanges(remoteHistory) > acceptableIdentityChanges) {
            if (localNodeIsMaster == false && remoteHistory == null) {
                logger.trace("Unable to get master history from {}}", master);
            } else {
                logger.trace("The master node {} thinks it is unstable", master);
            }
            stableMasterStatus = HealthStatus.YELLOW;
            summary = String.format(
                Locale.ROOT,
                "The cluster's master has alternated between %s and no master multiple times in the last %s",
                localMasterHistory.getNodes().stream().filter(Objects::nonNull).collect(Collectors.toSet()),
                localMasterHistory.getMaxHistoryAge()
            );
            impacts = getUnstableMasterImpacts();
            if (explain) {
                details.put(DETAILS_CURRENT_MASTER, new DiscoveryNodeXContentObject(localMasterHistory.getMostRecentMaster()));
                if (remoteHistoryException != null) {
                    details.put(DETAILS_EXCEPTION_FETCHING_HISTORY, remoteHistoryException.getMessage());
                    StringWriter stringWriter = new StringWriter();
                    remoteHistoryException.printStackTrace(new PrintWriter(stringWriter));
                    String remoteHistoryExceptionStackTrace = stringWriter.toString();
                    details.put(DETAILS_EXCEPTION_FETCHING_HISTORY_STACK_TRACE, remoteHistoryExceptionStackTrace);
                }
            }
            userActions = getContactSupportUserActions(explain);
        } else {
            logger.trace("This node thinks the master is unstable, but the master node {} thinks it is stable", master);
            stableMasterStatus = HealthStatus.GREEN;
            summary = "The cluster has a stable master node";
            impacts = List.of();
            userActions = List.of();
        }
        return createIndicator(
            stableMasterStatus,
            summary,
            explain ? new SimpleHealthIndicatorDetails(details) : HealthIndicatorDetails.EMPTY,
            impacts,
            userActions
        );
    }

    /**
     * Returns a HealthIndicatorResult for the case when the master is seen as stable
     * @return A HealthIndicatorResult for the case when the master is seen as stable (GREEN status, no impacts or details)
     */
    private HealthIndicatorResult getMasterIsStableResult(boolean explain, MasterHistory localMasterHistory) {
        HealthStatus stableMasterStatus = HealthStatus.GREEN;
        String summary = "The cluster has a stable master node";
        Collection<HealthIndicatorImpact> impacts = new ArrayList<>();
        List<UserAction> userActions = List.of();
        logger.trace("The cluster has a stable master node");
        Map<String, Object> details = getSimpleDetailsMap(explain, localMasterHistory);
        return createIndicator(stableMasterStatus, summary, new SimpleHealthIndicatorDetails(details), impacts, userActions);
    }

    /**
     * Returns the health result for the case when we have NOT seen a master recently (at some point in the last 30 seconds).
     * @param localMasterHistory The master history as seen from the local machine
     * @param explain Whether to calculate and include the details in the result
     * @return The HealthIndicatorResult for the given localMasterHistory
     */
    private HealthIndicatorResult calculateWhenHaveNotSeenMasterRecently(MasterHistory localMasterHistory, boolean explain) {
        // NOTE: The logic in this method will be implemented in a future PR
        HealthStatus stableMasterStatus = HealthStatus.RED;
        String summary = "Placeholder summary";
        Map<String, Object> details = new HashMap<>();
        Collection<HealthIndicatorImpact> impacts = getUnstableMasterImpacts();
        List<UserAction> userActions = getContactSupportUserActions(explain);
        return createIndicator(
            stableMasterStatus,
            summary,
            explain ? new SimpleHealthIndicatorDetails(details) : HealthIndicatorDetails.EMPTY,
            impacts,
            userActions
        );
    }

    private boolean hasSeenMasterInVeryRecentPast() {
        if (clusterService.state().nodes().getMasterNode() != null) {
            return true;
        }
        return masterHistoryService.getLocalMasterHistory().hasSeenMasterInLastNSeconds((int) veryRecentPast.seconds());
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
            if (masterHistoryService.getLocalMasterHistory().hasMasterGoneNullAtLeastNTimes(acceptableNullTransitions + 1)) {
                DiscoveryNode master = masterHistoryService.getLocalMasterHistory().getMostRecentNonNullMaster();
                if (master != null && clusterService.localNode().equals(master) == false) {
                    masterHistoryService.refreshRemoteMasterHistory(master);
                }
            }
        }
    }

    /**
     * XContentBuilder doesn't deal well with ToXContentFragments (which is what DiscoveryNodes are). Also XContentBuilder doesn't do well
     * with null values in lists. This object wraps the DiscoveryNode's XContent in a start and end object, and writes out nulls as
     * XContent nulls.
     */
    private record DiscoveryNodeXContentObject(DiscoveryNode master) implements ToXContentObject {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (master == null) {
                builder.nullValue();
            } else {
                builder.startObject();
                master.toXContent(builder, params);
                builder.endObject();
            }
            return builder;
        }
    }
}
