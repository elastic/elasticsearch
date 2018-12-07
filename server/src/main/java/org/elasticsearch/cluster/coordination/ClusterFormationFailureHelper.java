/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfiguration;
import org.elasticsearch.cluster.coordination.CoordinationState.VoteCollection;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING;
import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODE_COUNT_SETTING;

public class ClusterFormationFailureHelper {
    private static final Logger logger = LogManager.getLogger(ClusterFormationFailureHelper.class);

    public static final Setting<TimeValue> DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.cluster_formation_warning_timeout",
            TimeValue.timeValueMillis(10000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    private final Supplier<ClusterFormationState> clusterFormationStateSupplier;
    private final ThreadPool threadPool;
    private final TimeValue clusterFormationWarningTimeout;
    @Nullable // if no warning is scheduled
    private volatile WarningScheduler warningScheduler;

    public ClusterFormationFailureHelper(Settings settings, Supplier<ClusterFormationState> clusterFormationStateSupplier,
                                         ThreadPool threadPool) {
        this.clusterFormationStateSupplier = clusterFormationStateSupplier;
        this.threadPool = threadPool;
        this.clusterFormationWarningTimeout = DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING.get(settings);
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
            threadPool.scheduleUnlessShuttingDown(clusterFormationWarningTimeout, Names.GENERIC, new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.debug("unexpected exception scheduling cluster formation warning", e);
                }

                @Override
                protected void doRun() {
                    if (isActive()) {
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

    static class ClusterFormationState {
        private final Settings settings;
        private final ClusterState clusterState;
        private final List<TransportAddress> resolvedAddresses;
        private final List<DiscoveryNode> foundPeers;

        ClusterFormationState(Settings settings, ClusterState clusterState, List<TransportAddress> resolvedAddresses,
                              List<DiscoveryNode> foundPeers) {
            this.settings = settings;
            this.clusterState = clusterState;
            this.resolvedAddresses = resolvedAddresses;
            this.foundPeers = foundPeers;
        }

        String getDescription() {
            final List<String> clusterStateNodes
                = StreamSupport.stream(clusterState.nodes().spliterator(), false).map(DiscoveryNode::toString).collect(Collectors.toList());

            final String discoveryWillContinueDescription = String.format(Locale.ROOT,
                "discovery will continue using %s from hosts providers and %s from last-known cluster state",
                resolvedAddresses, clusterStateNodes);

            final String discoveryStateIgnoringQuorum = String.format(Locale.ROOT, "have discovered %s; %s",
                foundPeers, discoveryWillContinueDescription);

            if (clusterState.nodes().getLocalNode().isMasterNode() == false) {
                return String.format(Locale.ROOT, "master not discovered yet: %s", discoveryStateIgnoringQuorum);
            }

            if (clusterState.getLastAcceptedConfiguration().isEmpty()) {

                // TODO handle the case that there is a 6.x node around here, when rolling upgrades are supported

                final String bootstrappingDescription;

                if (INITIAL_MASTER_NODE_COUNT_SETTING.get(Settings.EMPTY).equals(INITIAL_MASTER_NODE_COUNT_SETTING.get(settings))
                    && INITIAL_MASTER_NODES_SETTING.get(Settings.EMPTY).equals(INITIAL_MASTER_NODES_SETTING.get(settings))) {
                    bootstrappingDescription = "cluster bootstrapping is disabled on this node";
                } else if (INITIAL_MASTER_NODES_SETTING.get(Settings.EMPTY).equals(INITIAL_MASTER_NODES_SETTING.get(settings))) {
                    bootstrappingDescription = String.format(Locale.ROOT,
                        "this node must discover at least [%d] master-eligible nodes to bootstrap a cluster",
                        INITIAL_MASTER_NODE_COUNT_SETTING.get(settings));
                } else if (INITIAL_MASTER_NODE_COUNT_SETTING.get(settings) <= INITIAL_MASTER_NODES_SETTING.get(settings).size()) {
                    // TODO update this when we can bootstrap on only a quorum of the initial nodes
                    bootstrappingDescription = String.format(Locale.ROOT,
                        "this node must discover master-eligible nodes %s to bootstrap a cluster",
                        INITIAL_MASTER_NODES_SETTING.get(settings));
                } else {
                    // TODO update this when we can bootstrap on only a quorum of the initial nodes
                    bootstrappingDescription = String.format(Locale.ROOT,
                        "this node must discover at least [%d] master-eligible nodes, including %s, to bootstrap a cluster",
                        INITIAL_MASTER_NODE_COUNT_SETTING.get(settings), INITIAL_MASTER_NODES_SETTING.get(settings));
                }

                return String.format(Locale.ROOT,
                    "master not discovered yet, this node has not previously joined a bootstrapped (v%d+) cluster, and %s: %s",
                    Version.V_6_6_0.major + 1, bootstrappingDescription, discoveryStateIgnoringQuorum);
            }

            assert clusterState.getLastCommittedConfiguration().isEmpty() == false;

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
            final String isQuorumOrNot
                = CoordinationState.isElectionQuorum(voteCollection, clusterState) ? "is a quorum" : "is not a quorum";

            return String.format(Locale.ROOT,
                "master not discovered or elected yet, an election requires %s, have discovered %s which %s; %s",
                quorumDescription, foundPeers, isQuorumOrNot, discoveryWillContinueDescription);
        }

        private String describeQuorum(VotingConfiguration votingConfiguration) {
            final Set<String> nodeIds = votingConfiguration.getNodeIds();
            assert nodeIds.isEmpty() == false;

            if (nodeIds.size() == 1) {
                return "a node with id " + nodeIds;
            } else if (nodeIds.size() == 2) {
                return "two nodes with ids " + nodeIds;
            } else {
                return "at least " + (nodeIds.size() / 2 + 1) + " nodes with ids from " + nodeIds;
            }
        }
    }
}
