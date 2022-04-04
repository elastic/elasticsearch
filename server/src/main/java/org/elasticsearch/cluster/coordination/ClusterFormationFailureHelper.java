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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.coordination.CoordinationState.VoteCollection;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
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

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING;
import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;

public class ClusterFormationFailureHelper {
    private static final Logger logger = LogManager.getLogger(ClusterFormationFailureHelper.class);

    public static final Setting<TimeValue> DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING = Setting.timeSetting(
        "discovery.cluster_formation_warning_timeout",
        TimeValue.timeValueMillis(10000),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );

    private final Supplier<ClusterFormationState> clusterFormationStateSupplier;
    private final ThreadPool threadPool;
    private final TimeValue clusterFormationWarningTimeout;
    private final Runnable logLastFailedJoinAttempt;
    @Nullable // if no warning is scheduled
    private volatile WarningScheduler warningScheduler;

    public ClusterFormationFailureHelper(
        Settings settings,
        Supplier<ClusterFormationState> clusterFormationStateSupplier,
        ThreadPool threadPool,
        Runnable logLastFailedJoinAttempt
    ) {
        this.clusterFormationStateSupplier = clusterFormationStateSupplier;
        this.threadPool = threadPool;
        this.clusterFormationWarningTimeout = DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING.get(settings);
        this.logLastFailedJoinAttempt = logLastFailedJoinAttempt;
    }

    public boolean isRunning() {
        return warningScheduler != null;
    }

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
            threadPool.scheduleUnlessShuttingDown(clusterFormationWarningTimeout, Names.CLUSTER_COORDINATION, new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.debug("unexpected exception scheduling cluster formation warning", e);
                }

                @Override
                protected void doRun() {
                    if (isActive()) {
                        logLastFailedJoinAttempt.run();
                        logger.warn(clusterFormationStateSupplier.get().getDescription());
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

    record ClusterFormationState(
        Settings settings,
        ClusterState clusterState,
        List<TransportAddress> resolvedAddresses,
        List<DiscoveryNode> foundPeers,
        long currentTerm,
        ElectionStrategy electionStrategy,
        StatusInfo statusInfo
    ) {

        String getDescription() {
            if (statusInfo.getStatus() == UNHEALTHY) {
                return String.format(Locale.ROOT, "this node is unhealthy: %s", statusInfo.getInfo());
            }

            final StringBuilder clusterStateNodes = new StringBuilder();
            DiscoveryNodes.addCommaSeparatedNodesWithoutAttributes(
                clusterState.nodes().getMasterNodes().values().iterator(),
                clusterStateNodes
            );

            final String discoveryWillContinueDescription = String.format(
                Locale.ROOT,
                "discovery will continue using %s from hosts providers and [%s] from last-known cluster state; "
                    + "node term %d, last-accepted version %d in term %d",
                resolvedAddresses,
                clusterStateNodes,
                currentTerm,
                clusterState.version(),
                clusterState.term()
            );

            final StringBuilder foundPeersDescription = new StringBuilder();
            DiscoveryNodes.addCommaSeparatedNodesWithoutAttributes(foundPeers.iterator(), foundPeersDescription);

            final String discoveryStateIgnoringQuorum = String.format(
                Locale.ROOT,
                "have discovered [%s]; %s",
                foundPeersDescription,
                discoveryWillContinueDescription
            );

            if (clusterState.nodes().getLocalNode().isMasterNode() == false) {
                return String.format(Locale.ROOT, "master not discovered yet: %s", discoveryStateIgnoringQuorum);
            }

            if (clusterState.getLastAcceptedConfiguration().isEmpty()) {

                final String bootstrappingDescription;

                if (INITIAL_MASTER_NODES_SETTING.get(Settings.EMPTY).equals(INITIAL_MASTER_NODES_SETTING.get(settings))) {
                    bootstrappingDescription = "[" + INITIAL_MASTER_NODES_SETTING.getKey() + "] is empty on this node";
                } else {
                    bootstrappingDescription = String.format(
                        Locale.ROOT,
                        "this node must discover master-eligible nodes %s to bootstrap a cluster",
                        INITIAL_MASTER_NODES_SETTING.get(settings)
                    );
                }

                return String.format(
                    Locale.ROOT,
                    "master not discovered yet, this node has not previously joined a bootstrapped cluster, and %s: %s",
                    bootstrappingDescription,
                    discoveryStateIgnoringQuorum
                );
            }

            assert clusterState.getLastCommittedConfiguration().isEmpty() == false;

            if (clusterState.getLastCommittedConfiguration().equals(VotingConfiguration.MUST_JOIN_ELECTED_MASTER)) {
                return String.format(
                    Locale.ROOT,
                    "master not discovered yet and this node was detached from its previous cluster, have discovered [%s]; %s",
                    foundPeersDescription,
                    discoveryWillContinueDescription
                );
            }

            final String quorumDescription;
            if (clusterState.getLastAcceptedConfiguration().equals(clusterState.getLastCommittedConfiguration())) {
                quorumDescription = describeQuorum(clusterState.getLastAcceptedConfiguration());
            } else {
                quorumDescription = describeQuorum(clusterState.getLastAcceptedConfiguration())
                    + " and "
                    + describeQuorum(clusterState.getLastCommittedConfiguration());
            }

            final VoteCollection voteCollection = new VoteCollection();
            foundPeers.forEach(voteCollection::addVote);
            final String haveDiscoveredQuorum = electionStrategy.isElectionQuorum(
                clusterState.nodes().getLocalNode(),
                currentTerm,
                clusterState.term(),
                clusterState.version(),
                clusterState.getLastCommittedConfiguration(),
                clusterState.getLastAcceptedConfiguration(),
                voteCollection
            ) ? "have discovered possible quorum" : "have only discovered non-quorum";

            return String.format(
                Locale.ROOT,
                "master not discovered or elected yet, an election requires %s, %s [%s]; %s",
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
    }
}
