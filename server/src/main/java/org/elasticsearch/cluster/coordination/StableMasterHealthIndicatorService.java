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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.PeerFinder;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.UserAction;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
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

    private final ClusterService clusterService;
    private final DiscoveryModule discoveryModule;
    private final MasterHistoryService masterHistoryService;
    private final TransportService transportService;

    /**
     * This is the amount of time we use to make the initial decision -- have we seen a master node in the very recent past?
     */
    private final TimeValue veryRecentPast;
    /**
     * If the master transitions from a non-null master to a null master at least this manhy timesThis is the number of times it starts
     * impacting the health status.
     */
    private final int unacceptableNullTransitions;
    /**
     * This is the number of times that it is OK for the master history to show a transition one non-null master to a different non-null
     * master before it starts impacting the health status.
     */
    private final int acceptableIdentityChanges;

    private static final Logger logger = LogManager.getLogger(StableMasterHealthIndicatorService.class);

    // This is the default amount of time we look back to see if we have had a master at all, before moving on with other checks
    private static final TimeValue DEFAULT_VERY_RECENT_PAST = new TimeValue(30, TimeUnit.SECONDS);
    private static final TimeValue SMALLEST_ALLOWED_VERY_RECENT_PAST = new TimeValue(1, TimeUnit.SECONDS);

    // This is the default number of times that it is OK to have a master go null. Any more than this will be reported as a problem
    private static final int DEFAULT_ACCEPTABLE_NULL_TRANSITIONS = 3;
    private static final int SMALLEST_ALLOWED_ACCEPTABLE_NULL_TRANSITIONS = 0;

    // This is the default number of times that it is OK to have a master change identity. Any more than this will be reported as a problem
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

    private List<Scheduler.Cancellable> clusterFormationInfoTasks = List.of();
    private final Map<DiscoveryNode, ClusterFormationStateOrException> nodeToClusterFormationStateMap = new HashMap<>();

    public StableMasterHealthIndicatorService(
        ClusterService clusterService,
        DiscoveryModule discoveryModule,
        MasterHistoryService masterHistoryService,
        TransportService transportService
    ) {
        this.clusterService = clusterService;
        this.discoveryModule = discoveryModule;
        this.masterHistoryService = masterHistoryService;
        this.transportService = transportService;
        this.veryRecentPast = VERY_RECENT_PAST_SETTING.get(clusterService.getSettings());
        this.unacceptableNullTransitions = ACCEPTABLE_NULL_TRANSITIONS_SETTING.get(clusterService.getSettings()) + 1;
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
        logger.trace("Have seen a master in the last {}): {}", veryRecentPast, localMasterHistory.getMostRecentNonNullMaster());
        final HealthIndicatorResult result;
        if (masterChanges > acceptableIdentityChanges) {
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
                    || MasterHistory.getNumberOfMasterIdentityChanges(remoteHistory) > acceptableIdentityChanges));
        if (masterConfirmedUnstable) {
            logger.trace("The master node {} thinks it is unstable", master);
            final HealthStatus stableMasterStatus = HealthStatus.YELLOW;
            String summary = String.format(
                Locale.ROOT,
                "The cluster's master has alternated between %s and no master multiple times in the last %s",
                localMasterHistory.getNodes().stream().filter(Objects::nonNull).collect(Collectors.toSet()),
                localMasterHistory.getMaxHistoryAge()
            );
            final Collection<HealthIndicatorImpact> impacts = getUnstableMasterImpacts();
            Map<String, Object> details;
            if (explain) {
                details = new HashMap<>();
                details.put(DETAILS_CURRENT_MASTER, new DiscoveryNodeXContentObject(localMasterHistory.getMostRecentMaster()));
                if (remoteHistoryException != null) {
                    details.put(DETAILS_EXCEPTION_FETCHING_HISTORY, remoteHistoryException.getMessage());
                    StringWriter stringWriter = new StringWriter();
                    remoteHistoryException.printStackTrace(new PrintWriter(stringWriter));
                    String remoteHistoryExceptionStackTrace = stringWriter.toString();
                    details.put(DETAILS_EXCEPTION_FETCHING_HISTORY_STACK_TRACE, remoteHistoryExceptionStackTrace);
                }
            } else {
                details = Map.of();
            }
            final List<UserAction> userActions = getContactSupportUserActions(explain);
            return createIndicator(
                stableMasterStatus,
                summary,
                explain ? new SimpleHealthIndicatorDetails(details) : HealthIndicatorDetails.EMPTY,
                impacts,
                userActions
            );
        } else {
            logger.trace("This node thinks the master is unstable, but the master node {} thinks it is stable", master);
            return getMasterIsStableResult(explain, localMasterHistory);
        }
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
    private HealthIndicatorResult calculateOnHaveNotSeenMasterRecently(MasterHistory localMasterHistory, boolean explain) {
        Collection<DiscoveryNode> masterEligibleNodes = getMasterEligibleNodes();
        HealthStatus stableMasterStatus;
        String summary;
        Map<String, Object> details = new HashMap<>();
        Collection<HealthIndicatorImpact> impacts = getUnstableMasterImpacts();
        List<UserAction> userActions = getContactSupportUserActions(explain);
        if (masterEligibleNodes.isEmpty()) {
            stableMasterStatus = HealthStatus.RED;
            summary = "No master eligible nodes found in the cluster";
            if (explain) {
                details.put(
                    "recent_masters",
                    localMasterHistory.getNodes().stream().filter(Objects::nonNull).map(DiscoveryNodeXContentObject::new).toList()
                );
                details.put("cluster_coordination", discoveryModule.getCoordinator().getClusterFormationState().getDescription());
            }
        } else {
            PeerFinder peerFinder = discoveryModule.getCoordinator().getPeerFinder();
            Optional<DiscoveryNode> currentMaster = peerFinder.getLeader();
            if (currentMaster.isPresent()) {
                stableMasterStatus = HealthStatus.RED;
                summary = String.format(
                    Locale.ROOT,
                    "%s has been elected master, but the node being queried, %s, is unable to join it",
                    currentMaster.get(),
                    clusterService.localNode()
                );
                if (explain) {
                    List<DiscoveryNode> recentMasters = localMasterHistory.getNodes();
                    details.put("current_master", new DiscoveryNodeXContentObject(currentMaster.get()));
                    // Having the nulls in the recent masters xcontent list is not helpful, so we filter them out:
                    details.put(
                        "recent_masters",
                        recentMasters.stream().filter(Objects::nonNull).map(DiscoveryNodeXContentObject::new).toList()
                    );
                    details.put("cluster_coordination", discoveryModule.getCoordinator().getClusterFormationState().getDescription());
                }
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
                            explain ? new SimpleHealthIndicatorDetails(details) : HealthIndicatorDetails.EMPTY,
                            impacts,
                            userActions
                        );
                    }
                }
                Map<DiscoveryNode, List<DiscoveryNode>> nodesNotDiscoveredMap = new HashMap<>();
                for (Map.Entry<DiscoveryNode, ClusterFormationStateOrException> entry : nodeToClusterFormationStateMap.entrySet()) {
                    ClusterFormationFailureHelper.ClusterFormationState clusterFormationState = entry.getValue().clusterFormationState();
                    List<DiscoveryNode> foundPeersOnNode = clusterFormationState.getFoundPeers();
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
                        String.format(Locale.ROOT, "Some master eligible nodes have not discovered other master eligible nodes"),
                        explain ? new SimpleHealthIndicatorDetails(details) : HealthIndicatorDetails.EMPTY,
                        impacts,
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
                        String.format(Locale.ROOT, "Master eligible nodes cannot form a quorum"),
                        explain ? new SimpleHealthIndicatorDetails(details) : HealthIndicatorDetails.EMPTY,
                        impacts,
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

        return createIndicator(
            stableMasterStatus,
            summary,
            explain ? new SimpleHealthIndicatorDetails(details) : HealthIndicatorDetails.EMPTY,
            impacts,
            userActions
        );
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
        return masterHistoryService.getLocalMasterHistory().hasSeenMasterInLastNSeconds((int) veryRecentPast.seconds());
    }

    private Collection<DiscoveryNode> getMasterEligibleNodes() {
        Set<DiscoveryNode> masterEligibleNodes = new HashSet<>();
        discoveryModule.getCoordinator().getFoundPeers().forEach(node -> {
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
            long startTime = System.nanoTime();
            transportService.openConnection(
                // Note: This connection must be explicitly closed below
                node,
                ConnectionProfile.buildDefaultConnectionProfile(clusterService.getSettings()),
                new ActionListener<>() {
                    @Override
                    public void onResponse(Transport.Connection connection) {
                        Version minSupportedVersion = Version.V_8_3_0;
                        if (connection.getVersion().onOrAfter(minSupportedVersion)) { // This was introduced in 8.3.0
                            logger.trace("Opened connection to {}, making master history request", node);
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
                                        logger.warn("Exception in master history request to master node", e);
                                        nodeToClusterFormationStateMap.put(node, new ClusterFormationStateOrException(e));
                                    }
                                }, connection::close), ClusterFormationInfoAction.Response::new)
                            );
                        } else {
                            connection.close();
                            logger.trace(
                                "Cannot get master history for {} because it is at version {} and {} is required",
                                node,
                                connection.getVersion(),
                                minSupportedVersion
                            );
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("Exception connecting to master node", e);
                        nodeToClusterFormationStateMap.put(node, new ClusterFormationStateOrException(e));
                    }
                }
            );
        }, 0, 10, TimeUnit.SECONDS));
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
