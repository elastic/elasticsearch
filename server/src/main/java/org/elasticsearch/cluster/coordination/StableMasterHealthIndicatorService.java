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
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.admin.cluster.coordination.ClusterFormationInfoAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.PeerFinder;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.UserAction;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
    private final Coordinator coordinator;
    private final MasterHistoryService masterHistoryService;
    private final TransportService transportService;

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

    // This is the default amount of time we look back to see if we have had a master at all, before moving on with other checks
    private static final TimeValue NODE_HAS_MASTER_LOOKUP_TIMEFRAME = new TimeValue(30, TimeUnit.SECONDS);
    private static final TimeValue SMALLEST_ALLOWED_HAS_MASTER_LOOKUP_TIMEFRAME = new TimeValue(1, TimeUnit.SECONDS);

    // Keys for the details map:
    private static final String DETAILS_CURRENT_MASTER = "current_master";
    private static final String DETAILS_RECENT_MASTERS = "recent_masters";
    private static final String DETAILS_EXCEPTION_FETCHING_HISTORY = "exception_fetching_history";

    // Impacts of having an unstable master:
    private static final String UNSTABLE_MASTER_INGEST_IMPACT = "The cluster cannot create, delete, or rebalance indices, and cannot "
        + "insert or update documents.";
    private static final String UNSTABLE_MASTER_DEPLOYMENT_MANAGEMENT_IMPACT = "Scheduled tasks such as Watcher, ILM, and SLM will not "
        + "work. The _cat APIs will not work.";
    private static final String UNSTABLE_MASTER_BACKUP_IMPACT = "Snapshot and restore will not work.";

    /**
     * This is the list of the impacts to be reported when the master node is determined to be unstable.
     */
    private static final List<HealthIndicatorImpact> UNSTABLE_MASTER_IMPACTS = List.of(
        new HealthIndicatorImpact(1, UNSTABLE_MASTER_INGEST_IMPACT, List.of(ImpactArea.INGEST)),
        new HealthIndicatorImpact(1, UNSTABLE_MASTER_DEPLOYMENT_MANAGEMENT_IMPACT, List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)),
        new HealthIndicatorImpact(3, UNSTABLE_MASTER_BACKUP_IMPACT, List.of(ImpactArea.BACKUP))
    );

    public static final Setting<TimeValue> NODE_HAS_MASTER_LOOKUP_TIMEFRAME_SETTING = Setting.timeSetting(
        "health.master_history.has_master_lookup_timeframe",
        NODE_HAS_MASTER_LOOKUP_TIMEFRAME,
        SMALLEST_ALLOWED_HAS_MASTER_LOOKUP_TIMEFRAME,
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

    private List<Scheduler.Cancellable> clusterFormationInfoTasks = List.of();
    private final Map<DiscoveryNode, ClusterFormationStateOrException> nodeToClusterFormationStateMap = new HashMap<>();

    public StableMasterHealthIndicatorService(
        ClusterService clusterService,
        Coordinator coordinator,
        MasterHistoryService masterHistoryService,
        TransportService transportService
    ) {
        this.clusterService = clusterService;
        this.coordinator = coordinator;
        this.masterHistoryService = masterHistoryService;
        this.transportService = transportService;
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
        if (hasSeenMasterInVeryRecentPast()) {
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
        DiscoveryNode masterNode = localMasterHistory.getMostRecentMaster();
        List<DiscoveryNode> recentNonNullMasters = localMasterHistory.getNodes().stream().filter(Objects::nonNull).toList();
        Map<String, String> masterNodeMap = new HashMap<>();
        masterNodeMap.put("node_id", masterNode == null ? null : masterNode.getId());
        masterNodeMap.put("name", masterNode == null ? null : masterNode.getName());
        List<Map<String, String>> recentMastersMaps = recentNonNullMasters.stream()
            .map(recentMaster -> Map.of("node_id", recentMaster.getId(), "name", recentMaster.getName()))
            .toList();
        Map<String, Object> details = Map.of(DETAILS_CURRENT_MASTER, masterNodeMap, DETAILS_RECENT_MASTERS, recentMastersMaps);
        return new HealthIndicatorDetails(details);
    }

    private HealthIndicatorDetails getDetails(boolean explain, MasterHistory localMasterHistory, String clusterCoordinationMessage) {
        if (explain == false) {
            return HealthIndicatorDetails.EMPTY;
        }
        List<DiscoveryNode> recentNonNullMasters = localMasterHistory.getNodes().stream().filter(Objects::nonNull).toList();
        List<Map<String, String>> recentMastersMaps = recentNonNullMasters.stream()
            .map(recentMaster -> Map.of("node_id", recentMaster.getId(), "name", recentMaster.getName()))
            .toList();
        Map<String, Object> details = Map.of(DETAILS_RECENT_MASTERS, recentMastersMaps, "cluster_coordination", clusterCoordinationMessage);
        return new HealthIndicatorDetails(details);
    }

    private HealthIndicatorDetails getDetails(
        boolean explain,
        MasterHistory localMasterHistory,
        @Nullable Exception exceptionFetchingHistory
    ) {
        if (explain == false) {
            return HealthIndicatorDetails.EMPTY;
        }
        DiscoveryNode masterNode = localMasterHistory.getMostRecentMaster();
        Map<String, String> masterNodeMap = new HashMap<>();
        masterNodeMap.put("node_id", masterNode == null ? null : masterNode.getId());
        masterNodeMap.put("name", masterNode == null ? null : masterNode.getName());
        final Map<String, Object> details;
        if (exceptionFetchingHistory == null) {
            details = Map.of(DETAILS_CURRENT_MASTER, masterNodeMap);
        } else {
            Map<String, String> exceptionMap = Map.of(
                "message",
                exceptionFetchingHistory.getMessage(),
                "stack_trace",
                getStackTraceString(exceptionFetchingHistory)
            );
            details = Map.of(DETAILS_CURRENT_MASTER, masterNodeMap, DETAILS_EXCEPTION_FETCHING_HISTORY, exceptionMap);
        }
        return new HealthIndicatorDetails(details);
    }

    private static String getStackTraceString(Exception exception) {
        StringWriter stringWriter = new StringWriter();
        exception.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
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
        return getDetails(explain, localMasterHistory, remoteHistoryException);
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
        Collection<DiscoveryNode> masterEligibleNodes = getMasterEligibleNodes();
        HealthStatus stableMasterStatus;
        String summary;
        HealthIndicatorDetails details = HealthIndicatorDetails.EMPTY;
        List<UserAction> userActions = getContactSupportUserActions(explain);
        if (masterEligibleNodes.isEmpty()) {
            stableMasterStatus = HealthStatus.RED;
            summary = "No master eligible nodes found in the cluster";
            details = getDetails(explain, localMasterHistory, coordinator.getClusterFormationState().getDescription());
        } else {
            PeerFinder peerFinder = coordinator.getPeerFinder();
            Optional<DiscoveryNode> currentMaster = peerFinder.getLeader();
            if (currentMaster.isPresent()) {
                stableMasterStatus = HealthStatus.RED;
                summary = String.format(
                    Locale.ROOT,
                    "%s has been elected master, but the node being queried, %s, is unable to join it",
                    currentMaster.get(),
                    clusterService.localNode()
                );
                details = getDetails(explain, localMasterHistory, coordinator.getClusterFormationState().getDescription());
            } else if (clusterService.localNode().isMasterNode() == false) { // none is elected master and we aren't master eligible
                // Use StableMasterHealthIndicatorServiceAction
                stableMasterStatus = HealthStatus.RED;
                summary = "No node is elected master, and this node is not master eligible. Reaching out to other nodes";
            } else { // none is elected master and we are master eligible
                for (Map.Entry<DiscoveryNode, ClusterFormationStateOrException> entry : nodeToClusterFormationStateMap.entrySet()) {
                    if (entry.getValue().exception() != null) {
                        return createIndicator(
                            HealthStatus.RED,
                            String.format(Locale.ROOT, "Exception reaching out to %s: %s", entry.getKey(), entry.getValue().exception()),
                            details,
                            UNSTABLE_MASTER_IMPACTS,
                            userActions
                        );
                    }
                }
                Map<DiscoveryNode, List<DiscoveryNode>> nodesNotDiscoveredMap = new HashMap<>();
                for (Map.Entry<DiscoveryNode, ClusterFormationStateOrException> entry : nodeToClusterFormationStateMap.entrySet()) {
                    ClusterFormationFailureHelper.ClusterFormationState clusterFormationState = entry.getValue().clusterFormationState();
                    List<DiscoveryNode> foundPeersOnNode = clusterFormationState.foundPeers();
                    if (foundPeersOnNode.containsAll(masterEligibleNodes) == false) {
                        List<DiscoveryNode> nodesNotDiscovered = masterEligibleNodes.stream()
                            .filter(node -> foundPeersOnNode.contains(node) == false)
                            .toList();
                        nodesNotDiscoveredMap.put(entry.getKey(), nodesNotDiscovered);
                    }
                }
                if (nodesNotDiscoveredMap.isEmpty() == false) {
                    return createIndicator(
                        HealthStatus.RED,
                        "Some master eligible nodes have not discovered other master eligible nodes",
                        details,
                        UNSTABLE_MASTER_IMPACTS,
                        userActions
                    );
                }
                List<String> quorumProblems = new ArrayList<>();
                for (Map.Entry<DiscoveryNode, ClusterFormationStateOrException> entry : nodeToClusterFormationStateMap.entrySet()) {
                    ClusterFormationFailureHelper.ClusterFormationState clusterFormationState = entry.getValue().clusterFormationState();
                    if (clusterFormationState.hasDiscoveredQuorum() == false) {
                        quorumProblems.add(clusterFormationState.getDescription());
                    }
                }
                if (quorumProblems.isEmpty() == false) {
                    return createIndicator(
                        HealthStatus.RED,
                        "Master eligible nodes cannot form a quorum",
                        details,
                        UNSTABLE_MASTER_IMPACTS,
                        userActions
                    );
                }
                stableMasterStatus = HealthStatus.RED;
                summary = "Something is very wrong";
            }
            // If one of them is elected master
            // Else if none is elected master and we aren't master eligible
            // Else if none is elected master and we are master eligible
        }

        return createIndicator(stableMasterStatus, summary, details, UNSTABLE_MASTER_IMPACTS, userActions);
    }

    /**
     * This returns true if this node has seen a master node within the last few seconds
     * @return true if this node has seen a master node within the last few seconds, false otherwise
     */
    private boolean hasSeenMasterInVeryRecentPast() {
        // If there is currently a master, there's no point in looking at the history:
        if (clusterService.state().nodes().getMasterNode() != null) {
            return true;
        }
        return masterHistoryService.getLocalMasterHistory().hasSeenMasterInLastNSeconds((int) nodeHasMasterLookupTimeframe.seconds());
    }

    private Collection<DiscoveryNode> getMasterEligibleNodes() {
        Set<DiscoveryNode> masterEligibleNodes = new HashSet<>();
        coordinator.getFoundPeers().forEach(node -> {
            if (node.isMasterNode()) {
                masterEligibleNodes.add(node);
            }
        });
        if (clusterService.localNode().isMasterNode()) {
            masterEligibleNodes.add(clusterService.localNode());
        }
        return masterEligibleNodes;
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
        if (currentMaster == null && clusterService.localNode().isMasterNode()) {
            beginPollingClusterFormationInfo();
        } else { // if already polling, stop polling
            cancelPollingClusterFormationInfo();
        }
    }

    private void cancelPollingClusterFormationInfo() {
        clusterFormationInfoTasks.forEach(Scheduler.Cancellable::cancel);
    }

    private void beginPollingClusterFormationInfo() {
        cancelPollingClusterFormationInfo();
        clusterFormationInfoTasks = getMasterEligibleNodes().stream()
            .map(this::beginPollingClusterFormationInfo)
            .collect(Collectors.toList());
    }

    private Scheduler.Cancellable beginPollingClusterFormationInfo(DiscoveryNode node) {
        return Scheduler.wrapAsCancellable(transportService.getThreadPool().scheduler().scheduleAtFixedRate(() -> {
            Version minSupportedVersion = Version.V_8_4_0;
            if (node.getVersion().onOrAfter(minSupportedVersion)) { // This was introduced in 8.4.0
                logger.trace(
                    "Cannot get cluster coordination info for {} because it is at version {} and {} is required",
                    node,
                    node.getVersion(),
                    minSupportedVersion
                );
            } else {
                long startTime = System.nanoTime();
                transportService.connectToNode(
                    // Note: This connection must be explicitly closed below
                    node,
                    ConnectionProfile.buildDefaultConnectionProfile(clusterService.getSettings()),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Releasable connection) {
                            Version minSupportedVersion = Version.V_8_4_0;
                            logger.trace("Opened connection to {}, making cluster coordination info request", node);
                            // If we don't get a response in 10 seconds that is a failure worth capturing on its own:
                            final TimeValue transportTimeout = TimeValue.timeValueSeconds(10);
                            transportService.sendRequest(
                                node,
                                ClusterFormationInfoAction.NAME,
                                new ClusterFormationInfoAction.Request(),
                                TransportRequestOptions.timeout(transportTimeout),
                                new ActionListenerResponseHandler<>(ActionListener.runBefore(new ActionListener<>() {

                                    @Override
                                    public void onResponse(ClusterFormationInfoAction.Response response) {
                                        long endTime = System.nanoTime();
                                        logger.trace(
                                            "Received cluster coordination info from {} in {}",
                                            node,
                                            TimeValue.timeValueNanos(endTime - startTime)
                                        );
                                        nodeToClusterFormationStateMap.put(
                                            node,
                                            new ClusterFormationStateOrException(response.getClusterFormationState())
                                        );
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        logger.warn("Exception in cluster coordination info request to master node", e);
                                        nodeToClusterFormationStateMap.put(node, new ClusterFormationStateOrException(e));
                                    }
                                }, () -> {
                                    if (transportService.getLocalNode().equals(node) == false) {
                                        connection.close();
                                    }
                                }), ClusterFormationInfoAction.Response::new)
                            );
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.warn("Exception connecting to master node", e);
                            nodeToClusterFormationStateMap.put(node, new ClusterFormationStateOrException(e));
                        }
                    }
                );
            }
        }, 0, 10, TimeUnit.SECONDS));
    }

    record ClusterFormationStateOrException(
        ClusterFormationFailureHelper.ClusterFormationState clusterFormationState,
        Exception exception
    ) { // non-private
        // for testing

        public ClusterFormationStateOrException {
            if (clusterFormationState != null && exception != null) {
                throw new IllegalArgumentException("Cluster formation state and exception cannot both be non-null");
            }
        }

        ClusterFormationStateOrException(ClusterFormationFailureHelper.ClusterFormationState clusterFormationState) {
            this(clusterFormationState, null);
        }

        ClusterFormationStateOrException(Exception exception) {
            this(null, exception);
        }
    }
}
