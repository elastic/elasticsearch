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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.discovery.zen.ElectMasterService.MasterCandidate;
import org.elasticsearch.discovery.zen.UnicastZenPing;
import org.elasticsearch.discovery.zen.UnicastZenPing.UnicastPingRequest;
import org.elasticsearch.discovery.zen.UnicastZenPing.UnicastPingResponse;
import org.elasticsearch.discovery.zen.ZenPing.PingResponse;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.Math.max;
import static org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING;
import static org.elasticsearch.cluster.ClusterState.UNKNOWN_VERSION;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentSet;
import static org.elasticsearch.discovery.zen.ZenDiscovery.PING_TIMEOUT_SETTING;

/**
 * Deals with rolling upgrades of the cluster coordination layer. In mixed clusters we prefer to elect the older nodes, but
 * when the last old node shuts down then as long as there are enough new nodes we can assume that they form the whole cluster and
 * define them as the initial configuration.
 */
public class DiscoveryUpgradeService {

    private static Logger logger = LogManager.getLogger(DiscoveryUpgradeService.class);

    // how long to wait after activation before attempting to join a master or perform a bootstrap upgrade
    public static final Setting<TimeValue> BWC_PING_TIMEOUT_SETTING =
        Setting.timeSetting("discovery.zen.bwc_ping_timeout",
            PING_TIMEOUT_SETTING, TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    // whether to try and bootstrap all the discovered Zen2 nodes when the last Zen1 node leaves the cluster.
    public static final Setting<Boolean> ENABLE_UNSAFE_BOOTSTRAPPING_ON_UPGRADE_SETTING =
        Setting.boolSetting("discovery.zen.unsafe_rolling_upgrades_enabled", true, Setting.Property.NodeScope);

    private final ElectMasterService electMasterService;
    private final TransportService transportService;
    private final BooleanSupplier isBootstrappedSupplier;
    private final JoinHelper joinHelper;
    private final Supplier<Iterable<DiscoveryNode>> peersSupplier;
    private final Consumer<VotingConfiguration> initialConfigurationConsumer;
    private final TimeValue bwcPingTimeout;
    private final boolean enableUnsafeBootstrappingOnUpgrade;
    private final ClusterName clusterName;

    @Nullable // null if no active joining round
    private volatile JoiningRound joiningRound;

    public DiscoveryUpgradeService(Settings settings, ClusterSettings clusterSettings, TransportService transportService,
                                   BooleanSupplier isBootstrappedSupplier, JoinHelper joinHelper,
                                   Supplier<Iterable<DiscoveryNode>> peersSupplier,
                                   Consumer<VotingConfiguration> initialConfigurationConsumer) {
        assert Version.CURRENT.major == Version.V_6_6_0.major + 1 : "remove this service once unsafe upgrades are no longer needed";
        electMasterService = new ElectMasterService(settings);
        this.transportService = transportService;
        this.isBootstrappedSupplier = isBootstrappedSupplier;
        this.joinHelper = joinHelper;
        this.peersSupplier = peersSupplier;
        this.initialConfigurationConsumer = initialConfigurationConsumer;
        this.bwcPingTimeout = BWC_PING_TIMEOUT_SETTING.get(settings);
        this.enableUnsafeBootstrappingOnUpgrade = ENABLE_UNSAFE_BOOTSTRAPPING_ON_UPGRADE_SETTING.get(settings);
        this.clusterName = CLUSTER_NAME_SETTING.get(settings);

        clusterSettings.addSettingsUpdateConsumer(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING,
            electMasterService::minimumMasterNodes); // TODO reject update if the new value is too large
    }

    public void activate(Optional<DiscoveryNode> lastKnownLeader) {
        // called under coordinator mutex

        if (isBootstrappedSupplier.getAsBoolean()) {
            return;
        }

        assert lastKnownLeader.isPresent() == false || Coordinator.isZen1Node(lastKnownLeader.get()) : lastKnownLeader;
        // if there was a leader and it's not a old node then we must have been bootstrapped

        assert joiningRound == null : joiningRound;
        joiningRound = new JoiningRound(lastKnownLeader.isPresent());
        joiningRound.scheduleNextAttempt();
    }

    public void deactivate() {
        // called under coordinator mutex
        joiningRound = null;
    }

    /**
     * Waits for some number of calls to {@link ListenableCountDown#countDown()} and then notifies a listener. The listener
     * is only ever notified once, whether successful or not.
     */
    private static class ListenableCountDown {
        private final CountDown countDown;
        private final ActionListener<Void> listener;

        ListenableCountDown(int count, ActionListener<Void> listener) {
            this.countDown = new CountDown(count);
            this.listener = listener;
        }

        void onFailure(Exception e) {
            if (countDown.fastForward()) {
                listener.onFailure(e);
            }
        }

        void countDown() {
            if (countDown.countDown()) {
                listener.onResponse(null);
            }
        }
    }

    private class JoiningRound {
        private final boolean upgrading;

        JoiningRound(boolean upgrading) {
            this.upgrading = upgrading;
        }

        private boolean isRunning() {
            return joiningRound == this && isBootstrappedSupplier.getAsBoolean() == false;
        }

        void scheduleNextAttempt() {
            if (isRunning() == false) {
                return;
            }

            final ThreadPool threadPool = transportService.getThreadPool();
            threadPool.scheduleUnlessShuttingDown(bwcPingTimeout, Names.SAME, new Runnable() {

                @Override
                public void run() {
                    if (isRunning() == false) {
                        return;
                    }

                    final Set<DiscoveryNode> discoveryNodes = Stream.concat(StreamSupport.stream(peersSupplier.get().spliterator(), false),
                        Stream.of(transportService.getLocalNode())).filter(DiscoveryNode::isMasterNode).collect(Collectors.toSet());

                    // this set of nodes is reasonably fresh - the PeerFinder cleans up nodes to which the transport service is not
                    // connected each time it wakes up (every second by default)

                    logger.debug("nodes: {}", discoveryNodes);

                    if (electMasterService.hasEnoughMasterNodes(discoveryNodes)) {
                        if (discoveryNodes.stream().anyMatch(Coordinator::isZen1Node)) {
                            electBestOldMaster(discoveryNodes);
                        } else if (upgrading && enableUnsafeBootstrappingOnUpgrade) {
                            // no Zen1 nodes found, but the last-known master was a Zen1 node, so this is a rolling upgrade
                            transportService.getThreadPool().generic().execute(() -> {
                                try {
                                    initialConfigurationConsumer.accept(new VotingConfiguration(discoveryNodes.stream()
                                        .map(DiscoveryNode::getId).collect(Collectors.toSet())));
                                } catch (Exception e) {
                                    logger.debug("exception during bootstrapping upgrade, retrying", e);
                                } finally {
                                    scheduleNextAttempt();
                                }
                            });
                        } else {
                            scheduleNextAttempt();
                        }
                    } else {
                        scheduleNextAttempt();
                    }
                }

                /**
                 * Ping all the old discovered masters one more time to obtain their cluster state versions, and then vote for the best one.
                 * @param discoveryNodes The master nodes (old and new).
                 */
                private void electBestOldMaster(Set<DiscoveryNode> discoveryNodes) {
                    final Set<MasterCandidate> masterCandidates = newConcurrentSet();
                    final ListenableCountDown listenableCountDown
                        = new ListenableCountDown(discoveryNodes.size(), new ActionListener<Void>() {

                        @Override
                        public void onResponse(Void value) {
                            assert masterCandidates.size() == discoveryNodes.size()
                                : masterCandidates + " does not match " + discoveryNodes;

                            // TODO we shouldn't elect a master with a version that's older than ours
                            // If the only Zen1 nodes left are stale, and we can bootstrap, maybe we should bootstrap?
                            // Do we ever need to elect a freshly-started Zen1 node?
                            if (isRunning()) {
                                final MasterCandidate electedMaster = electMasterService.electMaster(masterCandidates);
                                logger.debug("elected {}, sending join", electedMaster);
                                joinHelper.sendJoinRequest(electedMaster.getNode(), Optional.empty(),
                                    JoiningRound.this::scheduleNextAttempt);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            scheduleNextAttempt();
                        }
                    });

                    boolean foundOldMaster = false;
                    for (final DiscoveryNode discoveryNode : discoveryNodes) {
                        assert discoveryNode.isMasterNode() : discoveryNode;
                        if (Coordinator.isZen1Node(discoveryNode)) {
                            foundOldMaster = true;
                            transportService.sendRequest(discoveryNode, UnicastZenPing.ACTION_NAME,
                                new UnicastPingRequest(0, TimeValue.ZERO,
                                    new PingResponse(createDiscoveryNodeWithImpossiblyHighId(transportService.getLocalNode()),
                                        null, clusterName, UNKNOWN_VERSION)),
                                TransportRequestOptions.builder().withTimeout(bwcPingTimeout).build(),
                                new TransportResponseHandler<UnicastPingResponse>() {
                                    @Override
                                    public void handleResponse(UnicastPingResponse response) {
                                        long clusterStateVersion = UNKNOWN_VERSION;
                                        for (PingResponse pingResponse : response.pingResponses) {
                                            if (discoveryNode.equals(pingResponse.node())) {
                                                clusterStateVersion
                                                    = max(clusterStateVersion, pingResponse.getClusterStateVersion());
                                            }
                                        }
                                        masterCandidates.add(new MasterCandidate(discoveryNode, clusterStateVersion));
                                        listenableCountDown.countDown();
                                    }

                                    @Override
                                    public void handleException(TransportException exp) {
                                        logger.debug(
                                            new ParameterizedMessage("unexpected exception when pinging {}", discoveryNode), exp);
                                        listenableCountDown.onFailure(exp);
                                    }

                                    @Override
                                    public String executor() {
                                        return Names.SAME;
                                    }

                                    @Override
                                    public UnicastPingResponse read(StreamInput in) throws IOException {
                                        return new UnicastPingResponse(in);
                                    }
                                });

                        } else {
                            masterCandidates.add(
                                new MasterCandidate(createDiscoveryNodeWithImpossiblyHighId(discoveryNode), UNKNOWN_VERSION));
                            listenableCountDown.countDown();
                        }
                    }
                    assert foundOldMaster;
                }

                @Override
                public String toString() {
                    return "discovery upgrade service retry";
                }
            });
        }
    }

    /**
     * Pre-7.0 nodes select the best master by comparing their IDs (as strings) and selecting the lowest one amongst those nodes with
     * the best cluster state version. We want 7.0+ nodes to participate in these elections in a mixed cluster but never to win one, so
     * we lie and claim to have an impossible ID that compares above all genuine IDs.
     */
    public static DiscoveryNode createDiscoveryNodeWithImpossiblyHighId(DiscoveryNode node) {
        // IDs are base-64-encoded UUIDs, which means they use the character set [0-9A-Za-z_-]. The highest character in this set is 'z',
        // and 'z' < '{', so by starting the ID with '{' we can be sure it's greater. This is terrible.
        return new DiscoveryNode(node.getName(), "{zen2}" + node.getId(), node.getEphemeralId(), node.getHostName(),
            node.getHostAddress(), node.getAddress(), node.getAttributes(), node.getRoles(), node.getVersion());
    }
}
