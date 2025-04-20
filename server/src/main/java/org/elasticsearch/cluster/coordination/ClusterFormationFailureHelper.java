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
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.coordination.CoordinationState.VoteCollection;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING;
import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;

/**
 * Handles periodic debug logging of information regarding why the cluster has failed to form.
 * Periodic logging begins once {@link #start()} is called, and ceases on {@link #stop()}.
 */
public class ClusterFormationFailureHelper {
    private static final Logger logger = LogManager.getLogger(ClusterFormationFailureHelper.class);

    /**
     * This time period controls how often warning log messages will be written if this node fails to join or form a cluster.
     */
    public static final Setting<TimeValue> DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING = Setting.timeSetting(
        "discovery.cluster_formation_warning_timeout",
        TimeValue.timeValueMillis(10000),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );

    private final Supplier<ClusterFormationState> clusterFormationStateSupplier;
    private final ThreadPool threadPool;
    private final Executor clusterCoordinationExecutor;
    private final TimeValue clusterFormationWarningTimeout;
    private final Runnable logLastFailedJoinAttempt;
    @Nullable // if no warning is scheduled
    private volatile WarningScheduler warningScheduler;
    private volatile boolean loggingEnabled;

    /**
     * Works with the {@link JoinHelper} to log the latest node-join attempt failure and cluster state debug information. Must call
     * {@link ClusterFormationState#start()} to begin.
     *
     * @param settings provides the period in which to log cluster formation errors.
     * @param clusterFormationStateSupplier information about the current believed cluster state (See {@link ClusterFormationState})
     * @param threadPool the thread pool on which to run debug logging
     * @param logLastFailedJoinAttempt invokes an instance of the JoinHelper to log the last encountered join failure
     *                                 (See {@link JoinHelper#logLastFailedJoinAttempt()})
     */
    public ClusterFormationFailureHelper(
        Settings settings,
        Supplier<ClusterFormationState> clusterFormationStateSupplier,
        ThreadPool threadPool,
        Runnable logLastFailedJoinAttempt
    ) {
        this.clusterFormationStateSupplier = clusterFormationStateSupplier;
        this.threadPool = threadPool;
        this.clusterCoordinationExecutor = threadPool.executor(Names.CLUSTER_COORDINATION);
        this.clusterFormationWarningTimeout = DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING.get(settings);
        this.logLastFailedJoinAttempt = logLastFailedJoinAttempt;
        this.loggingEnabled = true;
    }

    public void setLoggingEnabled(boolean enabled) {
        this.loggingEnabled = enabled;
    }

    public boolean isRunning() {
        return warningScheduler != null;
    }

    /**
     * Schedules a warning debug message to be logged in 'clusterFormationWarningTimeout' time, and periodically thereafter, until
     * {@link ClusterFormationFailureHelper#stop()} has been called.
     */
    public void start() {
        assert warningScheduler == null;
        warningScheduler = new WarningScheduler();
        warningScheduler.scheduleNextWarning();
    }

    public void stop() {
        warningScheduler = null;
    }

    private class WarningScheduler {

        private boolean isActive() {
            return warningScheduler == this;
        }

        void scheduleNextWarning() {
            threadPool.scheduleUnlessShuttingDown(clusterFormationWarningTimeout, clusterCoordinationExecutor, new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.debug("unexpected exception scheduling cluster formation warning", e);
                }

                @Override
                protected void doRun() {
                    if (isActive() && loggingEnabled) {
                        logLastFailedJoinAttempt.run();
                        logger.warn(
                            "{}; for troubleshooting guidance, see {}",
                            clusterFormationStateSupplier.get().getDescription(),
                            ReferenceDocs.DISCOVERY_TROUBLESHOOTING
                        );
                    }
                }

                @Override
                public void onAfter() {
                    if (isActive()) {
                        scheduleNextWarning();
                    }
                }

                @Override
                public String toString() {
                    return "emit warning if cluster not formed";
                }
            });
        }
    }

    /**
     * This record provides node state information that can be used to determine why cluster formation has failed.
     */
    public record ClusterFormationState(
        List<String> initialMasterNodesSetting,
        DiscoveryNode localNode,
        Map<String, DiscoveryNode> masterEligibleNodes,
        long clusterStateVersion,
        long acceptedTerm,
        VotingConfiguration lastAcceptedConfiguration,
        VotingConfiguration lastCommittedConfiguration,
        List<TransportAddress> resolvedAddresses,
        List<DiscoveryNode> foundPeers,
        Set<DiscoveryNode> mastersOfPeers,
        long currentTerm,
        boolean hasDiscoveredQuorum,
        StatusInfo statusInfo,
        List<JoinStatus> inFlightJoinStatuses
    ) implements Writeable {

        public ClusterFormationState(
            Settings settings,
            ClusterState clusterState,
            List<TransportAddress> resolvedAddresses,
            List<DiscoveryNode> foundPeers,
            Set<DiscoveryNode> mastersOfPeers,
            long currentTerm,
            ElectionStrategy electionStrategy,
            StatusInfo statusInfo,
            List<JoinStatus> inFlightJoinStatuses
        ) {
            this(
                INITIAL_MASTER_NODES_SETTING.get(settings),
                clusterState.nodes().getLocalNode(),
                clusterState.nodes().getMasterNodes(),
                clusterState.version(),
                clusterState.term(),
                clusterState.getLastAcceptedConfiguration(),
                clusterState.getLastCommittedConfiguration(),
                resolvedAddresses,
                foundPeers,
                mastersOfPeers,
                currentTerm,
                calculateHasDiscoveredQuorum(
                    foundPeers,
                    electionStrategy,
                    clusterState.nodes().getLocalNode(),
                    currentTerm,
                    clusterState.term(),
                    clusterState.version(),
                    clusterState.getLastCommittedConfiguration(),
                    clusterState.getLastAcceptedConfiguration()
                ),
                statusInfo,
                inFlightJoinStatuses
            );
        }

        private static boolean calculateHasDiscoveredQuorum(
            List<DiscoveryNode> foundPeers,
            ElectionStrategy electionStrategy,
            DiscoveryNode localNode,
            long currentTerm,
            long acceptedTerm,
            long clusterStateVersion,
            VotingConfiguration lastCommittedConfiguration,
            VotingConfiguration lastAcceptedConfiguration
        ) {
            final VoteCollection voteCollection = new VoteCollection();
            foundPeers.forEach(voteCollection::addVote);
            return electionStrategy.isElectionQuorum(
                localNode,
                currentTerm,
                acceptedTerm,
                clusterStateVersion,
                lastCommittedConfiguration,
                lastAcceptedConfiguration,
                voteCollection
            );
        }

        public ClusterFormationState(StreamInput in) throws IOException {
            this(
                in.readStringCollectionAsList(),
                new DiscoveryNode(in),
                in.readMap(DiscoveryNode::new),
                in.readLong(),
                in.readLong(),
                new VotingConfiguration(in),
                new VotingConfiguration(in),
                in.readCollectionAsImmutableList(TransportAddress::new),
                in.readCollectionAsImmutableList(DiscoveryNode::new),
                in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)
                    ? in.readCollectionAsImmutableSet(DiscoveryNode::new)
                    : Set.of(),
                in.readLong(),
                in.readBoolean(),
                new StatusInfo(in),
                in.readCollectionAsList(JoinStatus::new)
            );
        }

        /**
         * This method provides a human-readable String describing why cluster formation failed.
         * @return A human-readable String describing why cluster formation failed
         */
        public String getDescription() {
            return getCoordinatorDescription() + getJoinStatusDescription();
        }

        private String getCoordinatorDescription() {
            if (statusInfo.getStatus() == UNHEALTHY) {
                return String.format(Locale.ROOT, "this node is unhealthy: %s", statusInfo.getInfo());
            }

            final StringBuilder clusterStateNodes = new StringBuilder();
            DiscoveryNodes.addCommaSeparatedNodesWithoutAttributes(masterEligibleNodes.values().iterator(), clusterStateNodes);

            final String discoveryWillContinueDescription = String.format(
                Locale.ROOT,
                "discovery will continue using %s from hosts providers and [%s] from last-known cluster state; "
                    + "node term %d, last-accepted version %d in term %d",
                resolvedAddresses,
                clusterStateNodes,
                currentTerm,
                clusterStateVersion,
                acceptedTerm
            );

            final StringBuilder foundPeersDescription = new StringBuilder("[");
            DiscoveryNodes.addCommaSeparatedNodesWithoutAttributes(foundPeers.iterator(), foundPeersDescription);
            if (mastersOfPeers.isEmpty()) {
                foundPeersDescription.append(']');
            } else {
                foundPeersDescription.append("] who claim current master to be [");
                DiscoveryNodes.addCommaSeparatedNodesWithoutAttributes(mastersOfPeers.iterator(), foundPeersDescription);
                foundPeersDescription.append(']');
            }

            final String discoveryStateIgnoringQuorum = String.format(
                Locale.ROOT,
                "have discovered %s; %s",
                foundPeersDescription,
                discoveryWillContinueDescription
            );

            if (localNode.isMasterNode() == false) {
                return String.format(Locale.ROOT, "master not discovered yet: %s", discoveryStateIgnoringQuorum);
            }

            if (lastAcceptedConfiguration.isEmpty()) {

                final String bootstrappingDescription;

                if (INITIAL_MASTER_NODES_SETTING.get(Settings.EMPTY).equals(initialMasterNodesSetting)) {
                    bootstrappingDescription = "[" + INITIAL_MASTER_NODES_SETTING.getKey() + "] is empty on this node";
                } else {
                    bootstrappingDescription = String.format(
                        Locale.ROOT,
                        "this node must discover master-eligible nodes %s to bootstrap a cluster",
                        initialMasterNodesSetting
                    );
                }

                return String.format(
                    Locale.ROOT,
                    "master not discovered yet, this node has not previously joined a bootstrapped cluster, and %s: %s",
                    bootstrappingDescription,
                    discoveryStateIgnoringQuorum
                );
            }

            assert lastCommittedConfiguration.isEmpty() == false;

            if (lastCommittedConfiguration.equals(VotingConfiguration.MUST_JOIN_ELECTED_MASTER)) {
                return String.format(
                    Locale.ROOT,
                    "master not discovered yet and this node was detached from its previous cluster, have discovered %s; %s",
                    foundPeersDescription,
                    discoveryWillContinueDescription
                );
            }

            final String quorumDescription;
            if (lastAcceptedConfiguration.equals(lastCommittedConfiguration)) {
                quorumDescription = describeQuorum(lastAcceptedConfiguration);
            } else {
                quorumDescription = describeQuorum(lastAcceptedConfiguration) + " and " + describeQuorum(lastCommittedConfiguration);
            }

            final VoteCollection voteCollection = new VoteCollection();
            foundPeers.forEach(voteCollection::addVote);
            final String haveDiscoveredQuorum = hasDiscoveredQuorum ? "have discovered possible quorum" : "have only discovered non-quorum";

            return String.format(
                Locale.ROOT,
                "master not discovered or elected yet, an election requires %s, %s %s; %s",
                quorumDescription,
                haveDiscoveredQuorum,
                foundPeersDescription,
                discoveryWillContinueDescription
            );
        }

        private static String describeQuorum(VotingConfiguration votingConfiguration) {
            final Set<String> nodeIds = votingConfiguration.getNodeIds();
            assert nodeIds.isEmpty() == false;
            final int requiredNodes = nodeIds.size() / 2 + 1;

            final Set<String> realNodeIds = new HashSet<>(nodeIds);
            realNodeIds.removeIf(ClusterBootstrapService::isBootstrapPlaceholder);
            assert requiredNodes <= realNodeIds.size() : nodeIds;

            if (nodeIds.size() == 1) {
                if (nodeIds.contains(GatewayMetaState.STALE_STATE_CONFIG_NODE_ID)) {
                    return "one or more nodes that have already participated as master-eligible nodes in the cluster but this node was "
                        + "not master-eligible the last time it joined the cluster";
                } else {
                    return "a node with id " + realNodeIds;
                }
            } else if (nodeIds.size() == 2) {
                return "two nodes with ids " + realNodeIds;
            } else {
                if (requiredNodes < realNodeIds.size()) {
                    return "at least " + requiredNodes + " nodes with ids from " + realNodeIds;
                } else {
                    return requiredNodes + " nodes with ids " + realNodeIds;
                }
            }
        }

        private String getJoinStatusDescription() {
            if (inFlightJoinStatuses.isEmpty()) {
                return "";
            }

            final var stringBuilder = new StringBuilder();
            inFlightJoinStatuses.stream()
                .sorted(Comparator.comparing(JoinStatus::age).reversed())
                .limit(10)
                .forEachOrdered(
                    joinStatus -> stringBuilder.append("; joining [")
                        .append(joinStatus.remoteNode().descriptionWithoutAttributes())
                        .append("] in term [")
                        .append(joinStatus.term())
                        .append("] has status [")
                        .append(joinStatus.message())
                        .append("] after [")
                        .append(timeValueWithMillis(joinStatus.age()))
                        .append("]")
                );
            return stringBuilder.toString();
        }

        private static String timeValueWithMillis(TimeValue timeValue) {
            final var millis = timeValue.millis();
            if (millis >= 1000) {
                return timeValue + "/" + millis + "ms";
            } else {
                return millis + "ms";
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(initialMasterNodesSetting);
            localNode.writeTo(out);
            out.writeMap(masterEligibleNodes, StreamOutput::writeWriteable);
            out.writeLong(clusterStateVersion);
            out.writeLong(acceptedTerm);
            lastAcceptedConfiguration.writeTo(out);
            lastCommittedConfiguration.writeTo(out);
            out.writeCollection(resolvedAddresses);
            out.writeCollection(foundPeers);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
                out.writeCollection(mastersOfPeers);
            }
            out.writeLong(currentTerm);
            out.writeBoolean(hasDiscoveredQuorum);
            statusInfo.writeTo(out);
            out.writeCollection(inFlightJoinStatuses);
        }
    }
}
