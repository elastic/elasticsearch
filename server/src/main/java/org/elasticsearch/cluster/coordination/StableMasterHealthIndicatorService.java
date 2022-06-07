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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.UserAction;

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

    public static final String NAME = "master_is_stable";
    private static final String HELP_URL = "https://ela.st/fix-master";

    private final ClusterService clusterService;
    private final MasterHistoryService masterHistoryService;
    /**
     * This is the amount of time we use to make the initial decision -- have we seen a master node in the very recent past?
     */
    private final TimeValue nodeHasMasterLookupTimeframe;
    /**
     * If the master transitions from a non-null master to a null master at least this many times it starts impacting the health status.
     */
    private final int unacceptableNullTransitions;
    /**
     * If the master transitions from one non-null master to a different non-null master at least this many times it starts impacting the
     * health status.
     */
    private final int unacceptableIdentityChanges;

    private static final Logger logger = LogManager.getLogger(StableMasterHealthIndicatorService.class);

    // Keys for the details map:
    private static final String DETAILS_CURRENT_MASTER = "current_master";
    private static final String DETAILS_RECENT_MASTERS = "recent_masters";
    private static final String DETAILS_EXCEPTION_FETCHING_HISTORY = "exception_fetching_history";

    // Impacts of having an unstable master:
    private static final String UNSTABLE_MASTER_INGEST_IMPACT = "The cluster cannot create, delete, or rebalance indices, and cannot "
        + "insert or update documents.";
    private static final String UNSTABLE_MASTER_DEPLOYMENT_MANAGEMENT_IMPACT = "Scheduled tasks such as Watcher, ILM, and SLM will not "
        + "work. The _cat APIs will not work.";
    private static final String UNSTABLE_MASTER_BACKUP_IMPACT = "Snapshot and restore will not work. Searchable snapshots cannot be "
        + "mounted.";

    /**
     * This is the list of the impacts to be reported when the master node is determined to be unstable.
     */
    private static final List<HealthIndicatorImpact> UNSTABLE_MASTER_IMPACTS = List.of(
        new HealthIndicatorImpact(1, UNSTABLE_MASTER_INGEST_IMPACT, List.of(ImpactArea.INGEST)),
        new HealthIndicatorImpact(1, UNSTABLE_MASTER_DEPLOYMENT_MANAGEMENT_IMPACT, List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)),
        new HealthIndicatorImpact(3, UNSTABLE_MASTER_BACKUP_IMPACT, List.of(ImpactArea.BACKUP))
    );

    /**
     * This is the default amount of time we look back to see if we have had a master at all, before moving on with other checks
     */
    public static final Setting<TimeValue> NODE_HAS_MASTER_LOOKUP_TIMEFRAME_SETTING = Setting.timeSetting(
        "health.master_history.has_master_lookup_timeframe",
        new TimeValue(30, TimeUnit.SECONDS),
        new TimeValue(1, TimeUnit.SECONDS),
        Setting.Property.NodeScope
    );

    /**
     * This is the number of times that it is not OK to have a master go null. This many transitions or more will be reported as a problem.
     */
    public static final Setting<Integer> NO_MASTER_TRANSITIONS_THRESHOLD_SETTING = Setting.intSetting(
        "health.master_history.no_master_transitions_threshold",
        4,
        0,
        Setting.Property.NodeScope
    );

    /**
     * This is the number of times that it is not OK to have a master change identity. This many changes or more will be reported as a
     * problem.
     */
    public static final Setting<Integer> IDENTITY_CHANGES_THRESHOLD_SETTING = Setting.intSetting(
        "health.master_history.identity_changes_threshold",
        4,
        0,
        Setting.Property.NodeScope
    );

    public StableMasterHealthIndicatorService(ClusterService clusterService, MasterHistoryService masterHistoryService) {
        this.clusterService = clusterService;
        this.masterHistoryService = masterHistoryService;
        this.nodeHasMasterLookupTimeframe = NODE_HAS_MASTER_LOOKUP_TIMEFRAME_SETTING.get(clusterService.getSettings());
        this.unacceptableNullTransitions = NO_MASTER_TRANSITIONS_THRESHOLD_SETTING.get(clusterService.getSettings());
        this.unacceptableIdentityChanges = IDENTITY_CHANGES_THRESHOLD_SETTING.get(clusterService.getSettings());
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
    public String helpURL() {
        return HELP_URL;
    }

    @Override
    public HealthIndicatorResult calculate(boolean explain) {
        MasterHistory localMasterHistory = masterHistoryService.getLocalMasterHistory();
        if (hasSeenMasterInHasMasterLookupTimeframe()) {
            return calculateOnHaveSeenMasterRecently(localMasterHistory, explain);
        } else {
            return calculateOnHaveNotSeenMasterRecently(localMasterHistory, explain);
        }
    }

    /**
     * Returns the health result for the case when we have seen a master recently (at some point in the last 30 seconds).
     * @param localMasterHistory The master history as seen from the local machine
     * @param explain Whether to calculate and include the details and user actions in the result
     * @return The HealthIndicatorResult for the given localMasterHistory
     */
    private HealthIndicatorResult calculateOnHaveSeenMasterRecently(MasterHistory localMasterHistory, boolean explain) {
        int masterChanges = MasterHistory.getNumberOfMasterIdentityChanges(localMasterHistory.getNodes());
        logger.trace(
            "Have seen a master in the last {}): {}",
            nodeHasMasterLookupTimeframe,
            localMasterHistory.getMostRecentNonNullMaster()
        );
        final HealthIndicatorResult result;
        if (masterChanges >= unacceptableIdentityChanges) {
            result = calculateOnMasterHasChangedIdentity(localMasterHistory, masterChanges, explain);
        } else if (localMasterHistory.hasMasterGoneNullAtLeastNTimes(unacceptableNullTransitions)) {
            result = calculateOnMasterHasFlappedNull(localMasterHistory, explain);
        } else {
            result = getMasterIsStableResult(explain, localMasterHistory);
        }
        return result;
    }

    /**
     * Returns the health result when we have detected locally that the master has changed identity repeatedly (by default more than 3
     * times in the last 30 minutes)
     * @param localMasterHistory The master history as seen from the local machine
     * @param masterChanges The number of times that the local machine has seen the master identity change in the last 30 minutes
     * @param explain Whether to calculate and include the details in the result
     * @return The HealthIndicatorResult for the given localMasterHistory
     */
    private HealthIndicatorResult calculateOnMasterHasChangedIdentity(
        MasterHistory localMasterHistory,
        int masterChanges,
        boolean explain
    ) {
        logger.trace("Have seen {} master changes in the last {}", masterChanges, localMasterHistory.getMaxHistoryAge());
        HealthStatus stableMasterStatus = HealthStatus.YELLOW;
        String summary = String.format(
            Locale.ROOT,
            "The elected master node has changed %d times in the last %s",
            masterChanges,
            localMasterHistory.getMaxHistoryAge()
        );
        HealthIndicatorDetails details = getDetails(explain, localMasterHistory);
        List<UserAction> userActions = getContactSupportUserActions(explain);
        return createIndicator(
            stableMasterStatus,
            summary,
            explain ? details : HealthIndicatorDetails.EMPTY,
            UNSTABLE_MASTER_IMPACTS,
            userActions
        );
    }

    /**
     * This returns HealthIndicatorDetails.EMPTY if explain is false, otherwise a HealthIndicatorDetails object containing only a
     * "current_master" object and a "recent_masters" array. The "current_master" object will have "node_id" and "name" fields for the
     * master node. Both will be null if the last-seen master was null. The "recent_masters" array will contain "recent_master" objects.
     * Each "recent_master" object will have "node_id" and "name" fields for the master node. These fields will never be null because
     * null masters are not written to this array.
     * @param explain If true, the HealthIndicatorDetails will contain "current_master" and "recent_masters". Otherwise it will be empty.
     * @param localMasterHistory The MasterHistory object to pull current and recent master info from
     * @return An empty HealthIndicatorDetails if explain is false, otherwise a HealthIndicatorDetails containing only "current_master"
     * and "recent_masters"
     */
    private HealthIndicatorDetails getDetails(boolean explain, MasterHistory localMasterHistory) {
        if (explain == false) {
            return HealthIndicatorDetails.EMPTY;
        }
        return (builder, params) -> {
            builder.startObject();
            DiscoveryNode masterNode = localMasterHistory.getMostRecentMaster();
            builder.object(DETAILS_CURRENT_MASTER, xContentBuilder -> {
                if (masterNode != null) {
                    builder.field("node_id", masterNode.getId());
                    builder.field("name", masterNode.getName());
                } else {
                    builder.nullField("node_id");
                    builder.nullField("name");
                }
            });
            List<DiscoveryNode> recentMasters = localMasterHistory.getNodes();
            builder.array(DETAILS_RECENT_MASTERS, arrayXContentBuilder -> {
                for (DiscoveryNode recentMaster : recentMasters) {
                    if (recentMaster != null) {
                        builder.startObject();
                        builder.field("node_id", recentMaster.getId());
                        builder.field("name", recentMaster.getName());
                        builder.endObject();
                    }
                }
            });
            return builder.endObject();
        };
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
                "The Elasticsearch cluster does not have a stable master node. Please contact Elastic Support "
                    + "(https://support.elastic.co) to discuss available options.",
                null
            );
            UserAction userAction = new UserAction(contactSupport, null);
            return List.of(userAction);
        } else {
            return List.of();
        }
    }

    /**
     * Returns the health result when we have detected locally that the master has changed to null repeatedly (by default more than 3 times
     * in the last 30 minutes). This method attemtps to use the master history from a remote node to confirm what we are seeing locally.
     * If the information from the remote node confirms that the master history has been unstable, a YELLOW status is returned. If the
     * information from the remote node shows that the master history has been stable, then we assume that the problem is with this node
     * and a GREEN status is returned (the problems with this node will be covered in a different health indicator). If there had been
     * problems fetching the remote master history, the exception seen will be included in the details of the result.
     * @param localMasterHistory The master history as seen from the local machine
     * @param explain Whether to calculate and include the details in the result
     * @return The HealthIndicatorResult for the given localMasterHistory
     */
    private HealthIndicatorResult calculateOnMasterHasFlappedNull(MasterHistory localMasterHistory, boolean explain) {
        DiscoveryNode master = localMasterHistory.getMostRecentNonNullMaster();
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
        /*
         * If the local node is master, then we have a confirmed problem (since we now know that from this node's point of view the
         * master is unstable).
         * If the local node is not master but the remote history is null then we have a problem (since from this node's point of view the
         * master is unstable, and we were unable to get the master's own view of its history). It could just be a short-lived problem
         * though if the remote history has not arrived yet.
         * If the local node is not master and the master history from the master itself reports that the master has gone null repeatedly
         *  or changed identity repeatedly, then we have a problem (the master has confirmed what the local node saw).
         */
        boolean masterConfirmedUnstable = localNodeIsMaster
            || remoteHistoryException != null
            || (remoteHistory != null
                && (MasterHistory.hasMasterGoneNullAtLeastNTimes(remoteHistory, unacceptableNullTransitions)
                    || MasterHistory.getNumberOfMasterIdentityChanges(remoteHistory) >= unacceptableIdentityChanges));
        if (masterConfirmedUnstable) {
            logger.trace("The master node {} thinks it is unstable", master);
            String summary = String.format(
                Locale.ROOT,
                "The cluster's master has alternated between %s and no master multiple times in the last %s",
                localMasterHistory.getNodes().stream().filter(Objects::nonNull).collect(Collectors.toSet()),
                localMasterHistory.getMaxHistoryAge()
            );
            final HealthIndicatorDetails details = getHealthIndicatorDetailsOnMasterHasFlappedNull(
                explain,
                localMasterHistory,
                remoteHistoryException
            );
            final List<UserAction> userActions = getContactSupportUserActions(explain);
            return createIndicator(
                HealthStatus.YELLOW,
                summary,
                explain ? details : HealthIndicatorDetails.EMPTY,
                UNSTABLE_MASTER_IMPACTS,
                userActions
            );
        } else {
            logger.trace("This node thinks the master is unstable, but the master node {} thinks it is stable", master);
            return getMasterIsStableResult(explain, localMasterHistory);
        }
    }

    /**
     * Returns the health indicator details for the calculateOnMasterHasFlappedNull method. The top-level objects are "current_master" and
     * (optionally) "exception_fetching_history". The "current_master" object will have "node_id" and "name" fields for the master node.
     * Both will be null if the last-seen master was null.
     * @param explain If false, nothing is calculated and HealthIndicatorDetails.EMPTY is returned
     * @param localMasterHistory The localMasterHistory
     * @param remoteHistoryException An exception that was found when retrieving the remote master history. Can be null
     * @return The HealthIndicatorDetails
     */
    private HealthIndicatorDetails getHealthIndicatorDetailsOnMasterHasFlappedNull(
        boolean explain,
        MasterHistory localMasterHistory,
        @Nullable Exception remoteHistoryException
    ) {
        if (explain == false) {
            return HealthIndicatorDetails.EMPTY;
        }
        return (builder, params) -> {
            builder.startObject();
            DiscoveryNode masterNode = localMasterHistory.getMostRecentMaster();
            builder.object(DETAILS_CURRENT_MASTER, xContentBuilder -> {
                if (masterNode != null) {
                    builder.field("node_id", masterNode.getId());
                    builder.field("name", masterNode.getName());
                } else {
                    builder.nullField("node_id");
                    builder.nullField("name");
                }
            });
            if (remoteHistoryException != null) {
                builder.object(DETAILS_EXCEPTION_FETCHING_HISTORY, xContentBuilder -> {
                    builder.field("message", remoteHistoryException.getMessage());
                    StringWriter stringWriter = new StringWriter();
                    remoteHistoryException.printStackTrace(new PrintWriter(stringWriter));
                    String remoteHistoryExceptionStackTrace = stringWriter.toString();
                    builder.field("stack_trace", remoteHistoryExceptionStackTrace);
                });
            }
            return builder.endObject();
        };
    }

    /**
     * Returns a HealthIndicatorResult for the case when the master is seen as stable
     * @return A HealthIndicatorResult for the case when the master is seen as stable (GREEN status, no impacts or details)
     */
    private HealthIndicatorResult getMasterIsStableResult(boolean explain, MasterHistory localMasterHistory) {
        String summary = "The cluster has a stable master node";
        Collection<HealthIndicatorImpact> impacts = new ArrayList<>();
        List<UserAction> userActions = List.of();
        logger.trace("The cluster has a stable master node");
        HealthIndicatorDetails details = getDetails(explain, localMasterHistory);
        return createIndicator(HealthStatus.GREEN, summary, details, impacts, userActions);
    }

    /**
     * Returns the health result for the case when we have NOT seen a master recently (at some point in the last 30 seconds).
     * @param localMasterHistory The master history as seen from the local machine
     * @param explain Whether to calculate and include the details in the result
     * @return The HealthIndicatorResult for the given localMasterHistory
     */
    private HealthIndicatorResult calculateOnHaveNotSeenMasterRecently(MasterHistory localMasterHistory, boolean explain) {
        // NOTE: The logic in this method will be implemented in a future PR
        String summary = "No master has been observed recently";
        Map<String, Object> details = new HashMap<>();
        List<UserAction> userActions = getContactSupportUserActions(explain);
        return createIndicator(
            HealthStatus.RED,
            summary,
            explain ? new SimpleHealthIndicatorDetails(details) : HealthIndicatorDetails.EMPTY,
            UNSTABLE_MASTER_IMPACTS,
            userActions
        );
    }

    /**
     * This returns true if this node has seen a master node within the last few seconds
     * @return true if this node has seen a master node within the last few seconds, false otherwise
     */
    private boolean hasSeenMasterInHasMasterLookupTimeframe() {
        // If there is currently a master, there's no point in looking at the history:
        if (clusterService.state().nodes().getMasterNode() != null) {
            return true;
        }
        return masterHistoryService.getLocalMasterHistory().hasSeenMasterInLastNSeconds((int) nodeHasMasterLookupTimeframe.seconds());
    }

    /*
     * If we detect that the master has gone null 3 or more times (by default), we ask the MasterHistoryService to fetch the master
     * history as seen from the most recent master node so that it is ready in case a health API request comes in. The request to the
     * MasterHistoryService is made asynchronously, and populates the value that MasterHistoryService.getRemoteMasterHistory() will return.
     * The remote master history is ordinarily returned very quickly if it is going to be returned, so the odds are very good it will be
     * in place by the time a request for it comes in. If not, this indicator will briefly switch to yellow.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode currentMaster = event.state().nodes().getMasterNode();
        DiscoveryNode previousMaster = event.previousState().nodes().getMasterNode();
        if (currentMaster == null && previousMaster != null) {
            if (masterHistoryService.getLocalMasterHistory().hasMasterGoneNullAtLeastNTimes(unacceptableNullTransitions)) {
                DiscoveryNode master = masterHistoryService.getLocalMasterHistory().getMostRecentNonNullMaster();
                /*
                 * If the most recent master was this box, there is no point in making a transport request -- we already know what this
                 * box's view of the master history is
                 */
                if (master != null && clusterService.localNode().equals(master) == false) {
                    masterHistoryService.refreshRemoteMasterHistory(master);
                }
            }
        }
    }
}
