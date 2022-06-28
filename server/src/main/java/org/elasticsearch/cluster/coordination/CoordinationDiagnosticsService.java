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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This service reports the health of master stability.
 * If we have had a master within the last 30 seconds, and that master has not changed more than 3 times in the last 30 minutes, then
 * this will report GREEN.
 * If we have had a master within the last 30 seconds, but that master has changed more than 3 times in the last 30 minutes (and that is
 * confirmed by checking with the last-known master), then this will report YELLOW.
 * If we have not had a master within the last 30 seconds, then this will will report RED with one exception. That exception is when:
 * (1) no node is elected master, (2) this node is not master eligible, (3) some node is master eligible, (4) we ask a master-eligible node
 * to run this service, and (5) it comes back with a result that is not RED.
 * Since this service needs to be able to run when there is no master at all, it does not depend on the dedicated health node (which
 * requires the existence of a master).
 */
public class CoordinationDiagnosticsService implements ClusterStateListener {
    private final ClusterService clusterService;
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
        Coordinator coordinator,
        MasterHistoryService masterHistoryService
    ) {
        this.clusterService = clusterService;
        this.coordinator = coordinator;
        this.masterHistoryService = masterHistoryService;
        this.nodeHasMasterLookupTimeframe = NODE_HAS_MASTER_LOOKUP_TIMEFRAME_SETTING.get(clusterService.getSettings());
        this.unacceptableNullTransitions = NO_MASTER_TRANSITIONS_THRESHOLD_SETTING.get(clusterService.getSettings());
        this.unacceptableIdentityChanges = IDENTITY_CHANGES_THRESHOLD_SETTING.get(clusterService.getSettings());
        clusterService.addListener(this);
    }

    /**
     * This method calculates the master stability as seen from this node.
     * @param explain If true, the result will contain a non-empty CoordinationDiagnosticsDetails if the resulting status is non-GREEN
     * @return Information about the current stability of the master node, as seen from this node
     */
    public CoordinationDiagnosticsResult diagnoseMasterStability(boolean explain) {
        MasterHistory localMasterHistory = masterHistoryService.getLocalMasterHistory();
        if (hasSeenMasterInHasMasterLookupTimeframe()) {
            return diagnoseOnHaveSeenMasterRecently(localMasterHistory, explain);
        } else {
            return diagnoseOnHaveNotSeenMasterRecently(localMasterHistory, explain);
        }
    }

    /**
     * Returns the health result for the case when we have seen a master recently (at some point in the last 30 seconds).
     * @param localMasterHistory The master history as seen from the local machine
     * @param explain Whether to calculate and include the details and user actions in the result
     * @return The CoordinationDiagnosticsResult for the given localMasterHistory
     */
    private CoordinationDiagnosticsResult diagnoseOnHaveSeenMasterRecently(MasterHistory localMasterHistory, boolean explain) {
        int masterChanges = MasterHistory.getNumberOfMasterIdentityChanges(localMasterHistory.getNodes());
        logger.trace(
            "Have seen a master in the last {}): {}",
            nodeHasMasterLookupTimeframe,
            localMasterHistory.getMostRecentNonNullMaster()
        );
        final CoordinationDiagnosticsResult result;
        if (masterChanges >= unacceptableIdentityChanges) {
            result = diagnoseOnMasterHasChangedIdentity(localMasterHistory, masterChanges, explain);
        } else if (localMasterHistory.hasMasterGoneNullAtLeastNTimes(unacceptableNullTransitions)) {
            result = diagnoseOnMasterHasFlappedNull(localMasterHistory, explain);
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
     * @return The CoordinationDiagnosticsResult for the given localMasterHistory
     */
    private CoordinationDiagnosticsResult diagnoseOnMasterHasChangedIdentity(
        MasterHistory localMasterHistory,
        int masterChanges,
        boolean explain
    ) {
        logger.trace("Have seen {} master changes in the last {}", masterChanges, localMasterHistory.getMaxHistoryAge());
        CoordinationDiagnosticsStatus coordinationDiagnosticsStatus = CoordinationDiagnosticsStatus.YELLOW;
        String summary = String.format(
            Locale.ROOT,
            "The elected master node has changed %d times in the last %s",
            masterChanges,
            localMasterHistory.getMaxHistoryAge()
        );
        CoordinationDiagnosticsDetails details = getDetails(explain, localMasterHistory, null);
        return new CoordinationDiagnosticsResult(coordinationDiagnosticsStatus, summary, details);
    }

    /**
     * This returns CoordinationDiagnosticsDetails.EMPTY if explain is false, otherwise a CoordinationDiagnosticsDetails object
     * containing only a "current_master" object and a "recent_masters" array. The "current_master" object will have "node_id" and "name"
     * fields for the master node. Both will be null if the last-seen master was null. The "recent_masters" array will contain
     * "recent_master" objects. Each "recent_master" object will have "node_id" and "name" fields for the master node. These fields will
     * never be null because null masters are not written to this array.
     * @param explain If true, the CoordinationDiagnosticsDetails will contain "current_master" and "recent_masters". Otherwise it will
     *                be empty.
     * @param localMasterHistory The MasterHistory object to pull current and recent master info from
     * @return An empty CoordinationDiagnosticsDetails if explain is false, otherwise a CoordinationDiagnosticsDetails containing only
     * "current_master" and "recent_masters"
     */
    private CoordinationDiagnosticsDetails getDetails(
        boolean explain,
        MasterHistory localMasterHistory,
        @Nullable String clusterFormationMessage
    ) {
        if (explain == false) {
            return CoordinationDiagnosticsDetails.EMPTY;
        }
        DiscoveryNode masterNode = localMasterHistory.getMostRecentMaster();
        List<DiscoveryNode> recentNonNullMasters = localMasterHistory.getNodes().stream().filter(Objects::nonNull).toList();
        return new CoordinationDiagnosticsDetails(masterNode, recentNonNullMasters, null, null, clusterFormationMessage);
    }

    /**
     * Returns the health result when we have detected locally that the master has changed to null repeatedly (by default more than 3 times
     * in the last 30 minutes). This method attemtps to use the master history from a remote node to confirm what we are seeing locally.
     * If the information from the remote node confirms that the master history has been unstable, a YELLOW status is returned. If the
     * information from the remote node shows that the master history has been stable, then we assume that the problem is with this node
     * and a GREEN status is returned (the problems with this node will be covered in a separate health indicator). If there had been
     * problems fetching the remote master history, the exception seen will be included in the details of the result.
     * @param localMasterHistory The master history as seen from the local machine
     * @param explain Whether to calculate and include the details in the result
     * @return The CoordinationDiagnosticsResult for the given localMasterHistory
     */
    private CoordinationDiagnosticsResult diagnoseOnMasterHasFlappedNull(MasterHistory localMasterHistory, boolean explain) {
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
            final CoordinationDiagnosticsDetails details = getDetailsOnMasterHasFlappedNull(
                explain,
                localMasterHistory,
                remoteHistoryException
            );
            return new CoordinationDiagnosticsResult(CoordinationDiagnosticsStatus.YELLOW, summary, details);
        } else {
            logger.trace("This node thinks the master is unstable, but the master node {} thinks it is stable", master);
            return getMasterIsStableResult(explain, localMasterHistory);
        }
    }

    /**
     * Returns the details for the calculateOnMasterHasFlappedNull method. This method populates the CoordinationDiagnosticsDetails
     * with the currentMaster, and optionally the remoteExceptionMessage and remoteExceptionStackTrace.
     * @param explain If false, nothing is calculated and CoordinationDiagnosticsDetails.EMPTY is returned
     * @param localMasterHistory The localMasterHistory
     * @param remoteHistoryException An exception that was found when retrieving the remote master history. Can be null
     * @return The CoordinationDiagnosticsDetails
     */
    private CoordinationDiagnosticsDetails getDetailsOnMasterHasFlappedNull(
        boolean explain,
        MasterHistory localMasterHistory,
        @Nullable Exception remoteHistoryException
    ) {
        if (explain == false) {
            return CoordinationDiagnosticsDetails.EMPTY;
        }
        return new CoordinationDiagnosticsDetails(localMasterHistory.getMostRecentMaster(), remoteHistoryException);
    }

    /**
     * Returns a CoordinationDiagnosticsResult for the case when the master is seen as stable
     * @return A CoordinationDiagnosticsResult for the case when the master is seen as stable (GREEN status, no impacts or details)
     */
    private CoordinationDiagnosticsResult getMasterIsStableResult(boolean explain, MasterHistory localMasterHistory) {
        String summary = "The cluster has a stable master node";
        logger.trace("The cluster has a stable master node");
        CoordinationDiagnosticsDetails details = getDetails(explain, localMasterHistory, null);
        return new CoordinationDiagnosticsResult(CoordinationDiagnosticsStatus.GREEN, summary, details);
    }

    /**
     * Returns the health result for the case when we have NOT seen a master recently (at some point in the last 30 seconds).
     * @param localMasterHistory The master history as seen from the local machine
     * @param explain Whether to calculate and include the details in the result
     * @return The CoordinationDiagnosticsResult for the given localMasterHistory
     */
    private CoordinationDiagnosticsResult diagnoseOnHaveNotSeenMasterRecently(MasterHistory localMasterHistory, boolean explain) {
        Collection<DiscoveryNode> masterEligibleNodes = getMasterEligibleNodes();
        final CoordinationDiagnosticsResult result;
        boolean leaderHasBeenElected = coordinator.getPeerFinder().getLeader().isPresent();
        if (masterEligibleNodes.isEmpty() && leaderHasBeenElected == false) {
            result = getResultOnNoMasterEligibleNodes(localMasterHistory, explain);
        } else if (leaderHasBeenElected) {
            DiscoveryNode currentMaster = coordinator.getPeerFinder().getLeader().get();
            result = getResultOnCannotJoinLeader(localMasterHistory, currentMaster, explain);
        } else {
            // NOTE: The logic in this block will be implemented in a future PR
            result = new CoordinationDiagnosticsResult(
                CoordinationDiagnosticsStatus.RED,
                "No master has been observed recently",
                CoordinationDiagnosticsDetails.EMPTY
            );
        }
        return result;
    }

    /**
     * Creates a CoordinationDiagnosticsResult in the case that there has been no master in the last few seconds, there is no elected
     * master known, and there are no master eligible nodes. The status will be RED, and the details (if explain is true) will contain
     * the list of any masters seen previously and a description of known problems from this node's Coordinator.
     * @param localMasterHistory Used to pull recent master nodes for the details if explain is true
     * @param explain If true, details are returned
     * @return A CoordinationDiagnosticsResult with a RED status
     */
    private CoordinationDiagnosticsResult getResultOnNoMasterEligibleNodes(MasterHistory localMasterHistory, boolean explain) {
        String summary = "No master eligible nodes found in the cluster";
        CoordinationDiagnosticsDetails details = getDetails(
            explain,
            localMasterHistory,
            coordinator.getClusterFormationState().getDescription()
        );
        return new CoordinationDiagnosticsResult(CoordinationDiagnosticsStatus.RED, summary, details);
    }

    /**
     * Creates a CoordinationDiagnosticsResult in the case that there has been no master in the last few seconds in this node's cluster
     * state, but PeerFinder reports that there is an elected master. The assumption is that this node is having a problem joining the
     * elected master. The status will be RED, and the details (if explain is true) will contain the list of any masters seen previously
     * and a description of known problems from this node's Coordinator.
     * @param localMasterHistory Used to pull recent master nodes for the details if explain is true
     * @param currentMaster The node that PeerFinder reports as the elected master
     * @param explain If true, details are returned
     * @return A CoordinationDiagnosticsResult with a RED status
     */
    private CoordinationDiagnosticsResult getResultOnCannotJoinLeader(
        MasterHistory localMasterHistory,
        DiscoveryNode currentMaster,
        boolean explain
    ) {
        String summary = String.format(
            Locale.ROOT,
            "%s has been elected master, but the node being queried, %s, is unable to join it",
            currentMaster,
            clusterService.localNode()
        );
        CoordinationDiagnosticsDetails details = getDetails(
            explain,
            localMasterHistory,
            coordinator.getClusterFormationState().getDescription()
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
        @Nullable String clusterFormationDescription
    ) implements Writeable {

        public CoordinationDiagnosticsDetails(DiscoveryNode currentMaster, List<DiscoveryNode> recentMasters) {
            this(currentMaster, recentMasters, null, null, null);
        }

        public CoordinationDiagnosticsDetails(DiscoveryNode currentMaster, Exception remoteException) {
            this(currentMaster, null, remoteException == null ? null : remoteException.getMessage(), getStackTrace(remoteException), null);
        }

        public CoordinationDiagnosticsDetails(StreamInput in) throws IOException {
            this(readCurrentMaster(in), readRecentMasters(in), in.readOptionalString(), in.readOptionalString(), in.readOptionalString());
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
                recentMasters = in.readImmutableList(DiscoveryNode::new);
            } else {
                recentMasters = null;
            }
            return recentMasters;
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
                out.writeList(recentMasters);
            }
            out.writeOptionalString(remoteExceptionMessage);
            out.writeOptionalString(remoteExceptionStackTrace);
            out.writeOptionalString(clusterFormationDescription);
        }

    }
}
