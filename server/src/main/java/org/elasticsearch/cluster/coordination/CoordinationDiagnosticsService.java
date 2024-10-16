/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.coordination.ClusterFormationInfoAction;
import org.elasticsearch.action.admin.cluster.coordination.CoordinationDiagnosticsAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * This service reports the health of master stability.
 * If we have had a master within the last 30 seconds, and that master has not changed more than 3 times in the last 30 minutes, then
 * this will report GREEN.
 * If we have had a master within the last 30 seconds, but that master has changed more than 3 times in the last 30 minutes (and that is
 * confirmed by checking with the last-known master), then this will report YELLOW.
 * If we have not had a master within the last 30 seconds, then this will report RED with one exception. That exception is when:
 * (1) no node is elected master, (2) this node is not master eligible, (3) some node is master eligible, (4) we ask a master-eligible node
 * to run this service, and (5) it comes back with a result that is not RED.
 * Since this service needs to be able to run when there is no master at all, it does not depend on the dedicated health node (which
 * requires the existence of a master).
 */
public class CoordinationDiagnosticsService implements ClusterStateListener {
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final Executor clusterCoordinationExecutor;
    private final Coordinator coordinator;
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

    // ThreadLocal because our unit testing framework does not like sharing Randoms across threads
    private final ThreadLocal<Random> random = ThreadLocal.withInitial(Randomness::get);

    /*
     * This is a Map of tasks that are periodically reaching out to other master eligible nodes to get their ClusterFormationStates for
     * diagnosis. The key is the DiscoveryNode for the master eligible node being polled, and the value is a Cancellable.
     * The field is accessed (reads/writes) from multiple threads, but the reference itself is only ever changed on the cluster change
     * event thread.
     */
    // Non-private for testing
    volatile Map<DiscoveryNode, Scheduler.Cancellable> clusterFormationInfoTasks = null;
    /*
     * This field holds the results of the tasks in the clusterFormationInfoTasks field above. The field is accessed (reads/writes) from
     * multiple threads, but the reference itself is only ever changed on the cluster change event thread.
     */
    // Non-private for testing
    volatile ConcurrentMap<DiscoveryNode, ClusterFormationStateOrException> clusterFormationResponses = null;

    /*
     * This is a reference to the task that is periodically reaching out to a master eligible node to get its CoordinationDiagnosticsResult
     * for diagnosis. It is null when no polling is occurring.
     * The field is accessed (reads/writes) from multiple threads. It is only reassigned on the initialization thread and the cluster
     * change event thread.
     */
    volatile AtomicReference<Scheduler.Cancellable> remoteCoordinationDiagnosisTask = null;
    /*
     * This field holds the result of the task in the remoteCoordinationDiagnosisTask field above. The field is accessed
     * (reads/writes) from multiple threads, but is only ever reassigned on the initialization thread and the cluster change event thread.
     */
    volatile AtomicReference<RemoteMasterHealthResult> remoteCoordinationDiagnosisResult = null;

    /**
     * This is the amount of time that we wait before scheduling a remote request to gather diagnostic information. It is not
     * user-configurable, but is non-final so that integration tests don't have to waste 10 seconds.
     */
    // Non-private for testing
    static TimeValue remoteRequestInitialDelay = new TimeValue(10, TimeUnit.SECONDS);

    private static final Logger logger = LogManager.getLogger(CoordinationDiagnosticsService.class);

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

    public CoordinationDiagnosticsService(
        ClusterService clusterService,
        TransportService transportService,
        Coordinator coordinator,
        MasterHistoryService masterHistoryService
    ) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.clusterCoordinationExecutor = transportService.getThreadPool().executor(ThreadPool.Names.CLUSTER_COORDINATION);
        this.coordinator = coordinator;
        this.masterHistoryService = masterHistoryService;
        this.nodeHasMasterLookupTimeframe = NODE_HAS_MASTER_LOOKUP_TIMEFRAME_SETTING.get(clusterService.getSettings());
        this.unacceptableNullTransitions = NO_MASTER_TRANSITIONS_THRESHOLD_SETTING.get(clusterService.getSettings());
        this.unacceptableIdentityChanges = IDENTITY_CHANGES_THRESHOLD_SETTING.get(clusterService.getSettings());
    }

    /**
     * This method completes the initialization of the CoordinationDiagnosticsService. It kicks off polling for remote master stability
     * results on non-master-eligible nodes, and registers the service as a cluster service listener on all nodes.
     */
    public void start() {
        /*
         * This is called here to cover an edge case -- when there are master-eligible nodes in the cluster but none of them has been
         * elected master. In the most common case this node will receive a ClusterChangedEvent that results in this polling being
         * cancelled almost immediately. If that does not happen, then we do in fact need to be polling. Note that
         * beginPollingRemoteMasterStabilityDiagnostic results in several internal transport actions being called, so it must run in the
         * system context.
         */
        if (clusterService.localNode().isMasterNode() == false) {
            try (var ignored = transportService.getThreadPool().getThreadContext().newEmptySystemContext()) {
                beginPollingRemoteMasterStabilityDiagnostic();
            }
        }
        clusterService.addListener(this);
    }

    /**
     * This method calculates the master stability as seen from this node.
     * @param verbose If true, the result will contain a non-empty CoordinationDiagnosticsDetails if the resulting status is non-GREEN
     * @return Information about the current stability of the master node, as seen from this node
     */
    public CoordinationDiagnosticsResult diagnoseMasterStability(boolean verbose) {
        MasterHistory localMasterHistory = masterHistoryService.getLocalMasterHistory();
        if (hasSeenMasterInHasMasterLookupTimeframe()) {
            return diagnoseOnHaveSeenMasterRecently(localMasterHistory, verbose);
        } else {
            return diagnoseOnHaveNotSeenMasterRecently(localMasterHistory, verbose);
        }
    }

    /**
     * Returns the health result for the case when we have seen a master recently (at some point in the last 30 seconds).
     * @param localMasterHistory The master history as seen from the local machine
     * @param verbose Whether to calculate and include the details and user actions in the result
     * @return The CoordinationDiagnosticsResult for the given localMasterHistory
     */
    private CoordinationDiagnosticsResult diagnoseOnHaveSeenMasterRecently(MasterHistory localMasterHistory, boolean verbose) {
        int masterChanges = MasterHistory.getNumberOfMasterIdentityChanges(localMasterHistory.getNodes());
        logger.trace(
            "Have seen a master in the last {}): {}",
            nodeHasMasterLookupTimeframe,
            localMasterHistory.getMostRecentNonNullMaster()
        );
        final CoordinationDiagnosticsResult result;
        if (masterChanges >= unacceptableIdentityChanges) {
            result = diagnoseOnMasterHasChangedIdentity(localMasterHistory, masterChanges, verbose);
        } else if (localMasterHistory.hasMasterGoneNullAtLeastNTimes(unacceptableNullTransitions)) {
            result = diagnoseOnMasterHasFlappedNull(localMasterHistory, verbose);
        } else {
            result = getMasterIsStableResult(verbose, localMasterHistory);
        }
        return result;
    }

    /**
     * Returns the health result when we have detected locally that the master has changed identity repeatedly (by default more than 3
     * times in the last 30 minutes)
     * @param localMasterHistory The master history as seen from the local machine
     * @param masterChanges The number of times that the local machine has seen the master identity change in the last 30 minutes
     * @param verbose Whether to calculate and include the details in the result
     * @return The CoordinationDiagnosticsResult for the given localMasterHistory
     */
    private static CoordinationDiagnosticsResult diagnoseOnMasterHasChangedIdentity(
        MasterHistory localMasterHistory,
        int masterChanges,
        boolean verbose
    ) {
        logger.trace("Have seen {} master changes in the last {}", masterChanges, localMasterHistory.getMaxHistoryAge());
        CoordinationDiagnosticsStatus coordinationDiagnosticsStatus = CoordinationDiagnosticsStatus.YELLOW;
        String summary = String.format(
            Locale.ROOT,
            "The elected master node has changed %d times in the last %s",
            masterChanges,
            localMasterHistory.getMaxHistoryAge()
        );
        CoordinationDiagnosticsDetails details = getDetails(verbose, localMasterHistory, null, null);
        return new CoordinationDiagnosticsResult(coordinationDiagnosticsStatus, summary, details);
    }

    /**
     * This returns CoordinationDiagnosticsDetails.EMPTY if verbose is false, otherwise a CoordinationDiagnosticsDetails object
     * containing only a "current_master" object and a "recent_masters" array. The "current_master" object will have "node_id" and "name"
     * fields for the master node. Both will be null if the last-seen master was null. The "recent_masters" array will contain
     * "recent_master" objects. Each "recent_master" object will have "node_id" and "name" fields for the master node. These fields will
     * never be null because null masters are not written to this array.
     * @param verbose If true, the CoordinationDiagnosticsDetails will contain "current_master" and "recent_masters". Otherwise it will
     *                be empty.
     * @param localMasterHistory The MasterHistory object to pull current and recent master info from
     * @return An empty CoordinationDiagnosticsDetails if verbose is false, otherwise a CoordinationDiagnosticsDetails containing only
     * "current_master" and "recent_masters"
     */
    private static CoordinationDiagnosticsDetails getDetails(
        boolean verbose,
        MasterHistory localMasterHistory,
        @Nullable Exception remoteException,
        @Nullable Map<String, String> clusterFormationMessages
    ) {
        if (verbose == false) {
            return CoordinationDiagnosticsDetails.EMPTY;
        }
        DiscoveryNode masterNode = localMasterHistory.getMostRecentMaster();
        List<DiscoveryNode> recentNonNullMasters = localMasterHistory.getNodes().stream().filter(Objects::nonNull).toList();
        return new CoordinationDiagnosticsDetails(masterNode, recentNonNullMasters, remoteException, clusterFormationMessages);
    }

    /**
     * Returns the health result when we have detected locally that the master has changed to null repeatedly (by default more than 3 times
     * in the last 30 minutes). This method attempts to use the master history from a remote node to confirm what we are seeing locally.
     * If the information from the remote node confirms that the master history has been unstable, a YELLOW status is returned. If the
     * information from the remote node shows that the master history has been stable, then we assume that the problem is with this node
     * and a GREEN status is returned (the problems with this node will be covered in a separate health indicator). If there had been
     * problems fetching the remote master history, the exception seen will be included in the details of the result.
     * @param localMasterHistory The master history as seen from the local machine
     * @param verbose Whether to calculate and include the details in the result
     * @return The CoordinationDiagnosticsResult for the given localMasterHistory
     */
    private CoordinationDiagnosticsResult diagnoseOnMasterHasFlappedNull(MasterHistory localMasterHistory, boolean verbose) {
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
         * or changed identity repeatedly, then we have a problem (the master has confirmed what the local node saw).
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
            final CoordinationDiagnosticsDetails details = getDetails(verbose, localMasterHistory, remoteHistoryException, null);
            return new CoordinationDiagnosticsResult(CoordinationDiagnosticsStatus.YELLOW, summary, details);
        } else {
            logger.trace("This node thinks the master is unstable, but the master node {} thinks it is stable", master);
            return getMasterIsStableResult(verbose, localMasterHistory);
        }
    }

    /**
     * Returns a CoordinationDiagnosticsResult for the case when the master is seen as stable
     * @return A CoordinationDiagnosticsResult for the case when the master is seen as stable (GREEN status, no impacts or details)
     */
    private static CoordinationDiagnosticsResult getMasterIsStableResult(boolean verbose, MasterHistory localMasterHistory) {
        String summary = "The cluster has a stable master node";
        logger.trace("The cluster has a stable master node");
        CoordinationDiagnosticsDetails details = getDetails(verbose, localMasterHistory, null, null);
        return new CoordinationDiagnosticsResult(CoordinationDiagnosticsStatus.GREEN, summary, details);
    }

    /**
     * Returns the health result for the case when we have NOT seen a master recently (at some point in the last 30 seconds).
     * @param localMasterHistory The master history as seen from the local machine
     * @param verbose Whether to calculate and include the details in the result
     * @return The CoordinationDiagnosticsResult for the given localMasterHistory
     */
    private CoordinationDiagnosticsResult diagnoseOnHaveNotSeenMasterRecently(MasterHistory localMasterHistory, boolean verbose) {
        Collection<DiscoveryNode> masterEligibleNodes = getMasterEligibleNodes();
        final CoordinationDiagnosticsResult result;
        boolean clusterHasLeader = coordinator.getPeerFinder().getLeader().isPresent();
        boolean noLeaderAndNoMasters = clusterHasLeader == false && masterEligibleNodes.isEmpty();
        boolean isLocalNodeMasterEligible = clusterService.localNode().isMasterNode();
        if (noLeaderAndNoMasters) {
            result = getResultOnNoMasterEligibleNodes(localMasterHistory, verbose);
        } else if (clusterHasLeader) {
            DiscoveryNode currentMaster = coordinator.getPeerFinder().getLeader().get();
            result = getResultOnCannotJoinLeader(localMasterHistory, currentMaster, verbose);
        } else if (isLocalNodeMasterEligible == false) { // none is elected master and we aren't master eligible
            result = diagnoseOnHaveNotSeenMasterRecentlyAndWeAreNotMasterEligible(
                localMasterHistory,
                coordinator,
                nodeHasMasterLookupTimeframe,
                remoteCoordinationDiagnosisResult,
                verbose
            );
        } else { // none is elected master and we are master eligible
            result = diagnoseOnHaveNotSeenMasterRecentlyAndWeAreMasterEligible(
                localMasterHistory,
                masterEligibleNodes,
                coordinator,
                clusterFormationResponses,
                nodeHasMasterLookupTimeframe,
                verbose
            );
        }
        return result;
    }

    /**
     * This method handles the case when we have not had an elected master node recently, and we are on a node that is not
     * master-eligible. In this case we reach out to some master-eligible node in order to see what it knows about master stability.
     * @param localMasterHistory The master history, as seen from this node
     * @param coordinator The Coordinator for this node
     * @param nodeHasMasterLookupTimeframe The value of health.master_history.has_master_lookup_timeframe
     * @param remoteCoordinationDiagnosisResult A reference to the result of polling a master-eligible node for diagnostic information
     * @param verbose If true, details are returned
     * @return A CoordinationDiagnosticsResult that will be determined by the CoordinationDiagnosticsResult returned by the remote
     * master-eligible node
     */
    static CoordinationDiagnosticsResult diagnoseOnHaveNotSeenMasterRecentlyAndWeAreNotMasterEligible(
        MasterHistory localMasterHistory,
        Coordinator coordinator,
        TimeValue nodeHasMasterLookupTimeframe,
        AtomicReference<RemoteMasterHealthResult> remoteCoordinationDiagnosisResult,
        boolean verbose
    ) {
        RemoteMasterHealthResult remoteResultOrException = remoteCoordinationDiagnosisResult == null
            ? null
            : remoteCoordinationDiagnosisResult.get();
        final CoordinationDiagnosticsStatus status;
        final String summary;
        final CoordinationDiagnosticsDetails details;
        if (remoteResultOrException == null) {
            status = CoordinationDiagnosticsStatus.RED;
            summary = String.format(
                Locale.ROOT,
                "No master node observed in the last %s, and this node is not master eligible. Reaching out to a master-eligible node"
                    + " for more information",
                nodeHasMasterLookupTimeframe
            );
            if (verbose) {
                details = getDetails(
                    true,
                    localMasterHistory,
                    null,
                    Map.of(coordinator.getLocalNode().getId(), coordinator.getClusterFormationState().getDescription())
                );
            } else {
                details = CoordinationDiagnosticsDetails.EMPTY;
            }
        } else {
            DiscoveryNode remoteNode = remoteResultOrException.node;
            CoordinationDiagnosticsResult remoteResult = remoteResultOrException.result;
            Exception exception = remoteResultOrException.remoteException;
            if (remoteResult != null) {
                if (remoteResult.status().equals(CoordinationDiagnosticsStatus.GREEN) == false) {
                    status = remoteResult.status();
                    summary = remoteResult.summary();
                } else {
                    status = CoordinationDiagnosticsStatus.RED;
                    summary = String.format(
                        Locale.ROOT,
                        "No master node observed in the last %s from this node, but %s reports that the status is GREEN. This "
                            + "indicates that there is a discovery problem on %s",
                        nodeHasMasterLookupTimeframe,
                        remoteNode.getName(),
                        coordinator.getLocalNode().getName()
                    );
                }
                if (verbose) {
                    details = remoteResult.details();
                } else {
                    details = CoordinationDiagnosticsDetails.EMPTY;
                }
            } else {
                status = CoordinationDiagnosticsStatus.RED;
                summary = String.format(
                    Locale.ROOT,
                    "No master node observed in the last %s from this node, and received an exception while reaching out to %s for "
                        + "diagnosis",
                    nodeHasMasterLookupTimeframe,
                    remoteNode.getName()
                );
                if (verbose) {
                    details = getDetails(true, localMasterHistory, exception, null);
                } else {
                    details = CoordinationDiagnosticsDetails.EMPTY;
                }
            }
        }
        return new CoordinationDiagnosticsResult(status, summary, details);
    }

    /**
     * This method handles the case when we have not had an elected master node recently, and we are on a master-eligible node. In this
     * case we look at the cluster formation information from all master-eligible nodes, trying to understand if we have a discovery
     * problem, a problem forming a quorum, or something else.
     * @param localMasterHistory The master history, as seen from this node
     * @param masterEligibleNodes The known master eligible nodes in the cluster
     * @param coordinator The Coordinator for this node
     * @param clusterFormationResponses A map that contains the cluster formation information (or exception encountered while requesting
     *                                  it) from each master eligible node in the cluster
     * @param nodeHasMasterLookupTimeframe The value of health.master_history.has_master_lookup_timeframe
     * @param verbose If true, details are returned
     * @return A CoordinationDiagnosticsResult with a RED status
     */
    static CoordinationDiagnosticsResult diagnoseOnHaveNotSeenMasterRecentlyAndWeAreMasterEligible(
        MasterHistory localMasterHistory,
        Collection<DiscoveryNode> masterEligibleNodes,
        Coordinator coordinator,
        ConcurrentMap<DiscoveryNode, ClusterFormationStateOrException> clusterFormationResponses,
        TimeValue nodeHasMasterLookupTimeframe,
        boolean verbose

    ) {
        final CoordinationDiagnosticsResult result;
        /*
         * We want to make sure that the same elements are in this set every time we loop through it. We don't care if values are added
         * while we're copying it, which is why this is not synchronized. We only care that once we have a copy it is not changed.
         */
        final Map<DiscoveryNode, ClusterFormationStateOrException> nodeToClusterFormationResponses = clusterFormationResponses == null
            ? Map.of()
            : Map.copyOf(clusterFormationResponses);
        for (Map.Entry<DiscoveryNode, ClusterFormationStateOrException> entry : nodeToClusterFormationResponses.entrySet()) {
            Exception remoteException = entry.getValue().exception();
            if (remoteException != null) {
                return new CoordinationDiagnosticsResult(
                    CoordinationDiagnosticsStatus.RED,
                    String.format(
                        Locale.ROOT,
                        "No master node observed in the last %s, and an exception occurred while reaching out to %s for diagnosis",
                        nodeHasMasterLookupTimeframe,
                        entry.getKey().getName()
                    ),
                    getDetails(
                        verbose,
                        localMasterHistory,
                        remoteException,
                        Map.of(coordinator.getLocalNode().getId(), coordinator.getClusterFormationState().getDescription())
                    )
                );
            }
        }
        Map<DiscoveryNode, ClusterFormationFailureHelper.ClusterFormationState> nodeClusterFormationStateMap =
            nodeToClusterFormationResponses.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().clusterFormationState()));
        if (nodeClusterFormationStateMap.isEmpty()) {
            /*
            * The most likely reason we are here is that polling for cluster formation info never began because there has been no cluster
            * changed event because there has never been a master node. So we just use the local cluster formation state.
            */
            nodeClusterFormationStateMap = Map.of(coordinator.getLocalNode(), coordinator.getClusterFormationState());
        }
        Map<String, String> nodeIdToClusterFormationDescription = nodeClusterFormationStateMap.entrySet()
            .stream()
            .collect(Collectors.toMap(entry -> entry.getKey().getId(), entry -> entry.getValue().getDescription()));
        if (anyNodeInClusterReportsDiscoveryProblems(masterEligibleNodes, nodeClusterFormationStateMap)) {
            result = new CoordinationDiagnosticsResult(
                CoordinationDiagnosticsStatus.RED,
                String.format(
                    Locale.ROOT,
                    "No master node observed in the last %s, and some master eligible nodes are unable to discover other master "
                        + "eligible nodes",
                    nodeHasMasterLookupTimeframe
                ),
                getDetails(verbose, localMasterHistory, null, nodeIdToClusterFormationDescription)
            );
        } else {
            if (anyNodeInClusterReportsQuorumProblems(nodeClusterFormationStateMap)) {
                result = new CoordinationDiagnosticsResult(
                    CoordinationDiagnosticsStatus.RED,
                    String.format(
                        Locale.ROOT,
                        "No master node observed in the last %s, and the master eligible nodes are unable to form a quorum",
                        nodeHasMasterLookupTimeframe
                    ),
                    getDetails(verbose, localMasterHistory, null, nodeIdToClusterFormationDescription)
                );
            } else {
                result = new CoordinationDiagnosticsResult(
                    CoordinationDiagnosticsStatus.RED,
                    String.format(
                        Locale.ROOT,
                        "No master node observed in the last %s, and the cause has not been determined.",
                        nodeHasMasterLookupTimeframe
                    ),
                    getDetails(verbose, localMasterHistory, null, nodeIdToClusterFormationDescription)
                );
            }
        }
        return result;
    }

    /**
     * This method checks whether each master eligible node has discovered each of the other master eligible nodes. For the sake of this
     * method, a discovery problem is when the foundPeers of any ClusterFormationState on any node we have that information for does not
     * contain all of the nodes in the local coordinator.getFoundPeers().
     * @param masterEligibleNodes The collection of all master eligible nodes
     * @param nodeToClusterFormationStateMap A map of each master node to its ClusterFormationState
     * @return true if there are discovery problems, false otherwise
     */
    static boolean anyNodeInClusterReportsDiscoveryProblems(
        Collection<DiscoveryNode> masterEligibleNodes,
        Map<DiscoveryNode, ClusterFormationFailureHelper.ClusterFormationState> nodeToClusterFormationStateMap
    ) {
        Map<DiscoveryNode, Collection<DiscoveryNode>> nodesNotDiscoveredMap = new HashMap<>();
        for (Map.Entry<DiscoveryNode, ClusterFormationFailureHelper.ClusterFormationState> entry : nodeToClusterFormationStateMap
            .entrySet()) {
            Set<DiscoveryNode> foundPeersOnNode = new HashSet<>(entry.getValue().foundPeers());
            if (foundPeersOnNode.containsAll(masterEligibleNodes) == false) {
                Collection<DiscoveryNode> nodesNotDiscovered = masterEligibleNodes.stream()
                    .filter(node -> foundPeersOnNode.contains(node) == false)
                    .toList();
                nodesNotDiscoveredMap.put(entry.getKey(), nodesNotDiscovered);
            }
        }
        if (nodesNotDiscoveredMap.isEmpty()) {
            return false;
        } else {
            String nodeDiscoveryProblemsMessage = nodesNotDiscoveredMap.entrySet()
                .stream()
                .map(
                    entry -> String.format(
                        Locale.ROOT,
                        "%s cannot discover [%s]",
                        entry.getKey().getName(),
                        entry.getValue().stream().map(DiscoveryNode::getName).collect(Collectors.joining(", "))
                    )
                )
                .collect(Collectors.joining("; "));
            logger.debug("The following nodes report discovery problems: {}", nodeDiscoveryProblemsMessage);
            return true;
        }
    }

    /**
     * This method checks that each master eligible node in the quorum thinks that it can form a quorum. If there are nodes that report a
     * problem forming a quorum, this method returns true. This method determines whether a node thinks that a quorum can be formed by
     * checking the value of that node's ClusterFormationState.hasDiscoveredQuorum field.
     * @param nodeToClusterFormationStateMap A map of each master node to its ClusterFormationState
     * @return True if any nodes in nodeToClusterFormationStateMap report a problem forming a quorum, false otherwise.
     */
    static boolean anyNodeInClusterReportsQuorumProblems(
        Map<DiscoveryNode, ClusterFormationFailureHelper.ClusterFormationState> nodeToClusterFormationStateMap
    ) {
        Map<DiscoveryNode, String> quorumProblems = new HashMap<>();
        for (Map.Entry<DiscoveryNode, ClusterFormationFailureHelper.ClusterFormationState> entry : nodeToClusterFormationStateMap
            .entrySet()) {
            ClusterFormationFailureHelper.ClusterFormationState clusterFormationState = entry.getValue();
            if (clusterFormationState.hasDiscoveredQuorum() == false) {
                quorumProblems.put(entry.getKey(), clusterFormationState.getDescription());
            }
        }
        if (quorumProblems.isEmpty()) {
            return false;
        } else {
            String quorumProblemsMessage = quorumProblems.entrySet()
                .stream()
                .map(
                    entry -> String.format(
                        Locale.ROOT,
                        "%s reports that a quorum cannot be formed: [%s]",
                        entry.getKey().getName(),
                        entry.getValue()
                    )
                )
                .collect(Collectors.joining("; "));
            logger.debug("Some master eligible nodes report that a quorum cannot be formed: {}", quorumProblemsMessage);
            return true;
        }
    }

    /**
     * Creates a CoordinationDiagnosticsResult in the case that there has been no master in the last few seconds, there is no elected
     * master known, and there are no master eligible nodes. The status will be RED, and the details (if verbose is true) will contain
     * the list of any masters seen previously and a description of known problems from this node's Coordinator.
     * @param localMasterHistory Used to pull recent master nodes for the details if verbose is true
     * @param verbose If true, details are returned
     * @return A CoordinationDiagnosticsResult with a RED status
     */
    private CoordinationDiagnosticsResult getResultOnNoMasterEligibleNodes(MasterHistory localMasterHistory, boolean verbose) {
        String summary = "No master eligible nodes found in the cluster";
        CoordinationDiagnosticsDetails details = getDetails(
            verbose,
            localMasterHistory,
            null,
            Map.of(coordinator.getLocalNode().getId(), coordinator.getClusterFormationState().getDescription())
        );
        return new CoordinationDiagnosticsResult(CoordinationDiagnosticsStatus.RED, summary, details);
    }

    /**
     * Creates a CoordinationDiagnosticsResult in the case that there has been no master in the last few seconds in this node's cluster
     * state, but PeerFinder reports that there is an elected master. The assumption is that this node is having a problem joining the
     * elected master. The status will be RED, and the details (if verbose is true) will contain the list of any masters seen previously
     * and a description of known problems from this node's Coordinator.
     * @param localMasterHistory Used to pull recent master nodes for the details if verbose is true
     * @param currentMaster The node that PeerFinder reports as the elected master
     * @param verbose If true, details are returned
     * @return A CoordinationDiagnosticsResult with a RED status
     */
    private CoordinationDiagnosticsResult getResultOnCannotJoinLeader(
        MasterHistory localMasterHistory,
        DiscoveryNode currentMaster,
        boolean verbose
    ) {
        String summary = String.format(
            Locale.ROOT,
            "%s has been elected master, but the node being queried, %s, is unable to join it",
            currentMaster,
            clusterService.localNode()
        );
        CoordinationDiagnosticsDetails details = getDetails(
            verbose,
            localMasterHistory,
            null,
            Map.of(coordinator.getLocalNode().getId(), coordinator.getClusterFormationState().getDescription())
        );
        return new CoordinationDiagnosticsResult(CoordinationDiagnosticsStatus.RED, summary, details);
    }

    /**
     * Returns the master eligible nodes as found in this node's Coordinator, plus the local node if it is master eligible.
     * @return All known master eligible nodes in this cluster
     */
    private Collection<DiscoveryNode> getMasterEligibleNodes() {
        Set<DiscoveryNode> masterEligibleNodes = new HashSet<>();
        coordinator.getFoundPeers().forEach(node -> {
            if (node.isMasterNode()) {
                masterEligibleNodes.add(node);
            }
        });
        // Coordinator does not report the local node, so add it:
        if (clusterService.localNode().isMasterNode()) {
            masterEligibleNodes.add(clusterService.localNode());
        }
        return masterEligibleNodes;
    }

    /**
     * Returns a random master eligible node, or null if this node does not know about any master eligible nodes
     * @return A random master eligible node or null
     */
    // Non-private for unit testing
    @Nullable
    DiscoveryNode getRandomMasterEligibleNode() {
        Collection<DiscoveryNode> masterEligibleNodes = getMasterEligibleNodes();
        if (masterEligibleNodes.isEmpty()) {
            return null;
        }
        return masterEligibleNodes.toArray(new DiscoveryNode[0])[random.get().nextInt(masterEligibleNodes.size())];
    }

    /**
     * This returns true if this node has seen a master node within the last few seconds
     * @return true if this node has seen a master node within the last few seconds, false otherwise
     */
    private boolean hasSeenMasterInHasMasterLookupTimeframe() {
        return masterHistoryService.getLocalMasterHistory().hasSeenMasterInLastNSeconds((int) nodeHasMasterLookupTimeframe.seconds());
    }

    /*
     * If we detect that the master has gone null 3 or more times (by default), we ask the MasterHistoryService to fetch the master
     * history as seen from the most recent master node so that it is ready in case a health API request comes in. The request to the
     * MasterHistoryService is made asynchronously, and populates the value that MasterHistoryService.getRemoteMasterHistory() will return.
     * The remote master history is ordinarily returned very quickly if it is going to be returned, so the odds are very good it will be
     * in place by the time a request for it comes in. If not, this service's status will briefly switch to yellow.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode currentMaster = event.state().nodes().getMasterNode();
        DiscoveryNode previousMaster = event.previousState().nodes().getMasterNode();
        if ((currentMaster == null && previousMaster != null) || (currentMaster != null && previousMaster == null)) {
            if (masterHistoryService.getLocalMasterHistory().hasMasterGoneNullAtLeastNTimes(unacceptableNullTransitions)) {
                /*
                 * If the master node has been going to null repeatedly, we want to make a remote request to it to see what it thinks of
                 * master stability. We want to query the most recent master whether the current master has just transitioned to null or
                 * just transitioned from null to not null. The reason that we make the latter request is that sometimes when the elected
                 * master goes to null the most recent master is not responsive for the duration of the request timeout (for example if
                 * that node is in the middle of a long GC pause which would be both the reason for it not being master and the reason it
                 * does not respond quickly to transport requests).
                 */
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
            /*
             * This begins polling all master-eligible nodes for cluster formation information. However there's a 10-second delay
             * before it starts, so in the normal situation where during a master transition it flips from master1 -> null ->
             * master2 the polling tasks will be canceled before any requests are actually made.
             */
            beginPollingClusterFormationInfo();
        } else {
            cancelPollingClusterFormationInfo();
        }
        if (clusterService.localNode().isMasterNode() == false) {
            if (currentMaster == null) {
                beginPollingRemoteMasterStabilityDiagnostic();
            } else {
                cancelPollingRemoteMasterStabilityDiagnostic();
            }
        }
    }

    /**
     * This method begins polling all known master-eligible nodes for cluster formation information. After a 10-second initial delay, it
     * polls each node every 10 seconds until cancelPollingClusterFormationInfo() is called.
     */
    void beginPollingClusterFormationInfo() {
        assert ThreadPool.assertCurrentThreadPool(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME);
        assert ThreadPool.assertInSystemContext(transportService.getThreadPool());
        cancelPollingClusterFormationInfo();
        ConcurrentMap<DiscoveryNode, ClusterFormationStateOrException> responses = new ConcurrentHashMap<>();
        Map<DiscoveryNode, Scheduler.Cancellable> cancellables = new ConcurrentHashMap<>();
        /*
         * Assignment of clusterFormationInfoTasks must be done before the call to beginPollingClusterFormationInfo because it is used
         * asynchronously by rescheduleClusterFormationFetchConsumer, called from beginPollingClusterFormationInfo.
         */
        clusterFormationInfoTasks = cancellables;
        clusterFormationResponses = responses;
        beginPollingClusterFormationInfo(getMasterEligibleNodes(), responses::put, cancellables);
    }

    /**
     * This method returns quickly, but in the background schedules to query the remote node's cluster formation state in 10 seconds, and
     * repeats doing that until cancel() is called on all of the Cancellable that this method inserts into cancellables. This method
     * exists (rather than being just part of the beginPollingClusterFormationInfo() above) in order to facilitate unit testing.
     * @param nodeResponseConsumer A consumer for any results produced for a node by this method
     * @param cancellables The Map of Cancellables, one for each node being polled
     */
    // Non-private for testing
    void beginPollingClusterFormationInfo(
        Collection<DiscoveryNode> masterEligibleNodes,
        BiConsumer<DiscoveryNode, ClusterFormationStateOrException> nodeResponseConsumer,
        Map<DiscoveryNode, Scheduler.Cancellable> cancellables
    ) {
        masterEligibleNodes.forEach(masterEligibleNode -> {
            Consumer<ClusterFormationStateOrException> responseConsumer = result -> nodeResponseConsumer.accept(masterEligibleNode, result);
            try {
                cancellables.put(
                    masterEligibleNode,
                    fetchClusterFormationInfo(
                        masterEligibleNode,
                        responseConsumer.andThen(
                            rescheduleClusterFormationFetchConsumer(masterEligibleNode, responseConsumer, cancellables)
                        )
                    )
                );
            } catch (EsRejectedExecutionException e) {
                if (e.isExecutorShutdown()) {
                    logger.trace("Not rescheduling request for cluster coordination info because this node is being shutdown", e);
                } else {
                    throw e;
                }
            }
        });
    }

    /**
     * This wraps the responseConsumer in a Consumer that will run rescheduleClusterFormationFetchConsumer() after responseConsumer has
     * completed, adding the resulting Cancellable to cancellables.
     * @param masterEligibleNode The node being polled
     * @param responseConsumer The response consumer to be wrapped
     * @param cancellables The Map of Cancellables, one for each node being polled
     * @return
     */
    private Consumer<CoordinationDiagnosticsService.ClusterFormationStateOrException> rescheduleClusterFormationFetchConsumer(
        DiscoveryNode masterEligibleNode,
        Consumer<CoordinationDiagnosticsService.ClusterFormationStateOrException> responseConsumer,
        Map<DiscoveryNode, Scheduler.Cancellable> cancellables
    ) {
        return response -> {
            /*
             * If clusterFormationInfoTasks is null, that means that cancelPollingClusterFormationInfo() has been called, so we don't
             * want to run anything new, and we want to cancel anything that might still be running in our cancellables just to be safe.
             */
            if (clusterFormationInfoTasks != null) {
                /*
                 * If cancellables is not the same as clusterFormationInfoTasks, that means that the current polling track has been
                 * cancelled and a new polling track has been started. So we don't want to run anything new, and we want to cancel
                 * anything that might still be running in our cancellables just to be safe. Note that it is possible for
                 * clusterFormationInfoTasks to be null at this point (since it is assigned in a different thread), so it is important
                 * that we don't call equals on it.
                 */
                if (cancellables.equals(clusterFormationInfoTasks)) {
                    /*
                     * As mentioned in the comment in cancelPollingClusterFormationInfo(), there is a slim possibility here that we will
                     * add a task here for a poll that has already been cancelled. But when it completes and runs
                     * rescheduleClusterFormationFetchConsumer() we will then see that clusterFormationInfoTasks does not equal
                     * cancellables, so it will not be run again.
                     */
                    try {
                        cancellables.put(
                            masterEligibleNode,
                            fetchClusterFormationInfo(
                                masterEligibleNode,
                                responseConsumer.andThen(
                                    rescheduleClusterFormationFetchConsumer(masterEligibleNode, responseConsumer, cancellables)
                                )
                            )
                        );
                    } catch (EsRejectedExecutionException e) {
                        if (e.isExecutorShutdown()) {
                            logger.trace("Not rescheduling request for cluster coordination info because this node is being shutdown", e);
                        } else {
                            throw e;
                        }
                    }
                } else {
                    cancellables.values().forEach(Scheduler.Cancellable::cancel);
                }
            } else {
                cancellables.values().forEach(Scheduler.Cancellable::cancel);
            }
        };
    }

    void cancelPollingClusterFormationInfo() {
        assert ThreadPool.assertCurrentThreadPool(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME);
        if (clusterFormationInfoTasks != null) {
            /*
             * There is a slight risk here that a new Cancellable is added to clusterFormationInfoTasks after we begin iterating in the next
             * line. We are calling this an acceptable risk because it will result in an un-cancelled un-cancellable task, but it will not
             * reschedule itself so it will not be around long. It is possible that cancel() will be called on a Cancellable concurrently
             * by multiple threads, but that will not cause any problems.
             */
            clusterFormationInfoTasks.values().forEach(Scheduler.Cancellable::cancel);
            clusterFormationInfoTasks = null;
            clusterFormationResponses = null;
        }
    }

    /**
     * This method returns quickly, but in the background schedules to query the remote node's cluster formation state in 10 seconds
     * unless cancel() is called on the Cancellable that this method returns.
     * @param node The node to poll for cluster formation information
     * @param responseConsumer The consumer of the cluster formation info for the node, or the exception encountered while contacting it
     * @return A Cancellable for the task that is scheduled to fetch cluster formation information
     * @throws EsRejectedExecutionException If the task cannot be scheduled, possibly because the node is shutting down.
     */
    private Scheduler.Cancellable fetchClusterFormationInfo(
        DiscoveryNode node,
        Consumer<ClusterFormationStateOrException> responseConsumer
    ) {
        return sendTransportRequest(
            node,
            responseConsumer,
            ClusterFormationInfoAction.NAME,
            ClusterFormationInfoAction.Response::new,
            new ClusterFormationInfoAction.Request(),
            (response, e) -> {
                assert response != null || e != null : "a response or an exception must be provided";
                if (response != null) {
                    return new ClusterFormationStateOrException(response.getClusterFormationState());
                } else {
                    return new ClusterFormationStateOrException(e);
                }
            }
        );
    }

    void beginPollingRemoteMasterStabilityDiagnostic() {
        assert ThreadPool.assertInSystemContext(transportService.getThreadPool());
        AtomicReference<Scheduler.Cancellable> cancellableReference = new AtomicReference<>();
        AtomicReference<RemoteMasterHealthResult> resultReference = new AtomicReference<>();
        remoteCoordinationDiagnosisTask = cancellableReference;
        remoteCoordinationDiagnosisResult = resultReference;
        beginPollingRemoteMasterStabilityDiagnostic(resultReference::set, cancellableReference);
    }

    /**
     * This method returns quickly, but in the background schedules to query a remote master node's cluster diagnostics in 10 seconds, and
     * repeats doing that until cancelPollingRemoteMasterStabilityDiagnostic() is called. This method
     * exists (rather than being just part of the beginPollingRemoteMasterStabilityDiagnostic() above) in order to facilitate
     * unit testing.
     * @param responseConsumer A consumer for any results produced for a node by this method
     * @param cancellableReference The Cancellable reference to assign the current Cancellable for this polling attempt
     */
    // Non-private for testing
    void beginPollingRemoteMasterStabilityDiagnostic(
        Consumer<RemoteMasterHealthResult> responseConsumer,
        AtomicReference<Scheduler.Cancellable> cancellableReference
    ) {
        DiscoveryNode masterEligibleNode = getRandomMasterEligibleNode();
        try {
            cancellableReference.set(
                fetchCoordinationDiagnostics(
                    masterEligibleNode,
                    responseConsumer.andThen(rescheduleDiagnosticsFetchConsumer(responseConsumer, cancellableReference))
                )
            );
        } catch (EsRejectedExecutionException e) {
            if (e.isExecutorShutdown()) {
                logger.trace("Not rescheduling request for cluster coordination info because this node is being shutdown", e);
            } else {
                throw e;
            }
        }
    }

    /**
     * This wraps the responseConsumer in a Consumer that will run rescheduleDiagnosticsFetchConsumer() after responseConsumer has
     * completed, adding the resulting Cancellable to cancellableReference.
     * @param responseConsumer The response consumer to be wrapped
     * @param cancellableReference The Cancellable reference to assign the current Cancellable for this polling attempt
     * @return A wrapped Consumer that will run fetchCoordinationDiagnostics()
     */
    private Consumer<RemoteMasterHealthResult> rescheduleDiagnosticsFetchConsumer(
        Consumer<RemoteMasterHealthResult> responseConsumer,
        AtomicReference<Scheduler.Cancellable> cancellableReference
    ) {
        return response -> {
            /*
             * If the cancellableReference for this poll attempt is equal to remoteCoordinationDiagnosisTask, then that means that
             * this poll attempt is the current one. If they are not equal, that means that
             * cancelPollingRemoteMasterStabilityDiagnostic() has been called on this poll attempt but this thread is not yet
             * aware. So we cancel the Cancellable in cancellableReference if it is not null. Note that
             * remoteCoordinationDiagnosisTask can be null.
             */
            if (cancellableReference.equals(remoteCoordinationDiagnosisTask)) {
                /*
                 * Because this is not synchronized with the cancelPollingRemoteMasterStabilityDiagnostic() method, there is a
                 * slim chance that we will add a task here for a poll that has already been cancelled. But when it completes and runs
                 * rescheduleDiagnosticsFetchConsumer() we will then see that remoteCoordinationDiagnosisTask does not equal
                 * cancellableReference, so it will not be run again.
                 */
                try {
                    DiscoveryNode masterEligibleNode = getRandomMasterEligibleNode();
                    cancellableReference.set(
                        fetchCoordinationDiagnostics(
                            masterEligibleNode,
                            responseConsumer.andThen(rescheduleDiagnosticsFetchConsumer(responseConsumer, cancellableReference))
                        )
                    );
                } catch (EsRejectedExecutionException e) {
                    if (e.isExecutorShutdown()) {
                        logger.trace("Not rescheduling request for cluster coordination info because this node is being shutdown", e);
                    } else {
                        throw e;
                    }
                }
            } else {
                Scheduler.Cancellable cancellable = cancellableReference.get();
                if (cancellable != null) {
                    cancellable.cancel();
                }
            }
        };
    }

    /**
     * This method returns quickly, but in the background schedules to query the remote masterEligibleNode's cluster diagnostics in 10
     * seconds unless cancel() is called on the Cancellable that this method returns.
     * @param masterEligibleNode The masterEligibleNode to poll for cluster diagnostics. This masterEligibleNode can be null in the case
     *                           when there are not yet any master-eligible nodes known to this masterEligibleNode's PeerFinder.
     * @param responseConsumer The consumer of the cluster diagnostics for the masterEligibleNode, or the exception encountered while
     *                         contacting it
     * @return A Cancellable for the task that is scheduled to fetch cluster diagnostics
     */
    private Scheduler.Cancellable fetchCoordinationDiagnostics(
        @Nullable DiscoveryNode masterEligibleNode,
        Consumer<RemoteMasterHealthResult> responseConsumer
    ) {
        return sendTransportRequest(
            masterEligibleNode,
            responseConsumer,
            CoordinationDiagnosticsAction.NAME,
            CoordinationDiagnosticsAction.Response::new,
            new CoordinationDiagnosticsAction.Request(true),
            (response, e) -> {
                assert response != null || e != null : "a response or an exception must be provided";
                if (response != null) {
                    return new RemoteMasterHealthResult(masterEligibleNode, response.getCoordinationDiagnosticsResult(), null);
                } else {
                    return new RemoteMasterHealthResult(masterEligibleNode, null, e);
                }
            }
        );
    }

    /**
     * This method connects to masterEligibleNode and sends it a transport request for a response of type R. The response or exception
     * are transformed into a common type T with responseToResultFunction or exceptionToResultFunction, and then consumed by
     * responseConsumer. This method is meant to be used when there is potentially no elected master node, so it first calls
     * connectToNode before sending the request.
     * @param masterEligibleNode        The master eligible node to be queried, or null if we do not yet know of a master eligible node.
     *                                  If this is null, the responseConsumer will be given a null response
     * @param responseConsumer          The consumer of the transformed response
     * @param actionName                The name of the transport action
     * @param responseReader            How to deserialize the transport response
     * @param transportActionRequest    The ActionRequest to be sent
     * @param responseTransformationFunction A function that converts a response or exception to the response type expected by the
     *                                       responseConsumer
     * @return A Cancellable for the task that is scheduled to fetch the remote information
     */
    private <R extends ActionResponse, T> Scheduler.Cancellable sendTransportRequest(
        @Nullable DiscoveryNode masterEligibleNode,
        Consumer<T> responseConsumer,
        String actionName,
        Writeable.Reader<R> responseReader,
        ActionRequest transportActionRequest,
        BiFunction<R, Exception, T> responseTransformationFunction
    ) {
        ListenableFuture<Releasable> connectionListener = new ListenableFuture<>();
        ListenableFuture<R> fetchRemoteResultListener = new ListenableFuture<>();
        long startTimeMillis = transportService.getThreadPool().relativeTimeInMillis();
        connectionListener.addListener(ActionListener.wrap(releasable -> {
            if (masterEligibleNode == null) {
                Releasables.close(releasable);
                responseConsumer.accept(null);
            } else {
                logger.trace("Opened connection to {}, making transport request", masterEligibleNode);
                // If we don't get a response in 10 seconds that is a failure worth capturing on its own:
                final TimeValue transportTimeout = TimeValue.timeValueSeconds(10);
                transportService.sendRequest(
                    masterEligibleNode,
                    actionName,
                    transportActionRequest,
                    TransportRequestOptions.timeout(transportTimeout),
                    new ActionListenerResponseHandler<>(
                        ActionListener.runBefore(fetchRemoteResultListener, () -> Releasables.close(releasable)),
                        responseReader,
                        clusterCoordinationExecutor
                    )
                );
            }
        }, e -> {
            logger.warn("Exception connecting to master " + masterEligibleNode, e);
            responseConsumer.accept(responseTransformationFunction.apply(null, e));
        }));

        fetchRemoteResultListener.addListener(ActionListener.wrap(response -> {
            long endTimeMillis = transportService.getThreadPool().relativeTimeInMillis();
            logger.trace(
                "Received remote response from {} in {}",
                masterEligibleNode,
                TimeValue.timeValueMillis(endTimeMillis - startTimeMillis)
            );
            responseConsumer.accept(responseTransformationFunction.apply(response, null));
        }, e -> {
            logger.warn("Exception in remote request to master " + masterEligibleNode, e);
            responseConsumer.accept(responseTransformationFunction.apply(null, e));
        }));

        return transportService.getThreadPool().schedule(new Runnable() {
            @Override
            public void run() {
                if (masterEligibleNode == null) {
                    /*
                     * This node's PeerFinder hasn't yet discovered the master-eligible nodes. By notifying the responseConsumer with a null
                     * value we effectively do nothing, and allow this request to be rescheduled.
                     */
                    responseConsumer.accept(null);
                } else {
                    Version minSupportedVersion = Version.V_8_4_0;
                    if (masterEligibleNode.getVersion().onOrAfter(minSupportedVersion) == false) {
                        logger.trace(
                            "Cannot get remote result from {} because it is at version {} and {} is required",
                            masterEligibleNode,
                            masterEligibleNode.getVersion(),
                            minSupportedVersion
                        );
                    } else {
                        transportService.connectToNode(
                            // Note: This connection must be explicitly closed in the connectionListener
                            masterEligibleNode,
                            connectionListener
                        );
                    }
                }
            }

            @Override
            public String toString() {
                return "delayed retrieval of coordination diagnostics info from " + masterEligibleNode;
            }
        }, remoteRequestInitialDelay, clusterCoordinationExecutor);
    }

    void cancelPollingRemoteMasterStabilityDiagnostic() {
        assert ThreadPool.assertCurrentThreadPool(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME);
        if (remoteCoordinationDiagnosisTask != null) {
            Scheduler.Cancellable task = remoteCoordinationDiagnosisTask.get();
            if (task != null) {
                task.cancel();
            }
            remoteCoordinationDiagnosisResult = null;
            remoteCoordinationDiagnosisTask = null;
        }
    }

    // Non-private for testing
    record ClusterFormationStateOrException(
        ClusterFormationFailureHelper.ClusterFormationState clusterFormationState,
        Exception exception
    ) {
        ClusterFormationStateOrException {
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

    public record CoordinationDiagnosticsResult(
        CoordinationDiagnosticsStatus status,
        String summary,
        CoordinationDiagnosticsDetails details
    ) implements Writeable {

        public CoordinationDiagnosticsResult(StreamInput in) throws IOException {
            this(CoordinationDiagnosticsStatus.fromStreamInput(in), in.readString(), new CoordinationDiagnosticsDetails(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            status.writeTo(out);
            out.writeString(summary);
            details.writeTo(out);
        }
    }

    public enum CoordinationDiagnosticsStatus implements Writeable {
        GREEN,
        UNKNOWN,
        YELLOW,
        RED;

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        public static CoordinationDiagnosticsStatus fromStreamInput(StreamInput in) throws IOException {
            return in.readEnum(CoordinationDiagnosticsStatus.class);
        }
    }

    public record CoordinationDiagnosticsDetails(
        DiscoveryNode currentMaster,
        List<DiscoveryNode> recentMasters,
        @Nullable String remoteExceptionMessage,
        @Nullable String remoteExceptionStackTrace,
        @Nullable Map<String, String> nodeToClusterFormationDescriptionMap
    ) implements Writeable {
        public CoordinationDiagnosticsDetails(
            DiscoveryNode currentMaster,
            List<DiscoveryNode> recentMasters,
            Exception remoteException,
            Map<String, String> nodeToClusterFormationDescriptionMap
        ) {
            this(
                currentMaster,
                recentMasters,
                remoteException == null ? null : remoteException.getMessage(),
                getStackTrace(remoteException),
                nodeToClusterFormationDescriptionMap
            );
        }

        public CoordinationDiagnosticsDetails(StreamInput in) throws IOException {
            this(
                readCurrentMaster(in),
                readRecentMasters(in),
                in.readOptionalString(),
                in.readOptionalString(),
                readClusterFormationStates(in)
            );
        }

        private static DiscoveryNode readCurrentMaster(StreamInput in) throws IOException {
            boolean hasCurrentMaster = in.readBoolean();
            DiscoveryNode currentMaster;
            if (hasCurrentMaster) {
                currentMaster = new DiscoveryNode(in);
            } else {
                currentMaster = null;
            }
            return currentMaster;
        }

        private static List<DiscoveryNode> readRecentMasters(StreamInput in) throws IOException {
            boolean hasRecentMasters = in.readBoolean();
            List<DiscoveryNode> recentMasters;
            if (hasRecentMasters) {
                recentMasters = in.readCollectionAsImmutableList(DiscoveryNode::new);
            } else {
                recentMasters = null;
            }
            return recentMasters;
        }

        private static Map<String, String> readClusterFormationStates(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                return in.readMap(StreamInput::readString);
            } else {
                return Map.of();
            }
        }

        private static String getStackTrace(Exception e) {
            if (e == null) {
                return null;
            }
            StringWriter stringWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stringWriter));
            return stringWriter.toString();
        }

        public static final CoordinationDiagnosticsDetails EMPTY = new CoordinationDiagnosticsDetails(null, null, null, null, null);

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (currentMaster == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                currentMaster.writeTo(out);
            }
            if (recentMasters == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeCollection(recentMasters);
            }
            out.writeOptionalString(remoteExceptionMessage);
            out.writeOptionalString(remoteExceptionStackTrace);
            if (nodeToClusterFormationDescriptionMap == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeMap(nodeToClusterFormationDescriptionMap, StreamOutput::writeString);
            }
        }

    }

    // Non-private for testing:
    record RemoteMasterHealthResult(DiscoveryNode node, CoordinationDiagnosticsResult result, Exception remoteException) {
        public RemoteMasterHealthResult {
            if (node == null) {
                throw new IllegalArgumentException("Node cannot be null");
            }
            if (result == null && remoteException == null) {
                throw new IllegalArgumentException("Must provide a non-null value for one of result or remoteException");
            }
        }
    }
}
