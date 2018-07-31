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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.UnicastZenPing;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.cluster.coordination.RunnableUtils.labelRunnable;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

public abstract class PeerFinder extends AbstractLifecycleComponent {

    public static final String REQUEST_PEERS_ACTION_NAME = "internal:discovery/requestpeers";

    // the time between attempts to find all peers
    public static final Setting<TimeValue> DISCOVERY_FIND_PEERS_INTERVAL_SETTING =
        Setting.timeSetting("discovery.find_peers_interval",
            TimeValue.timeValueMillis(1000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    private final TimeValue findPeersDelay;
    private final TimeValue resolveTimeout;

    private final Object mutex = new Object();
    private final TransportService transportService;
    private final UnicastHostsProvider hostsProvider;
    private final FutureExecutor futureExecutor;
    private final TransportAddressConnector transportAddressConnector;
    private final Supplier<ExecutorService> executorServiceFactory;

    private final SetOnce<ExecutorService> executorService = new SetOnce<>();
    private Optional<ActivePeerFinder> peerFinder = Optional.empty();

    PeerFinder(Settings settings, TransportService transportService, UnicastHostsProvider hostsProvider,
               FutureExecutor futureExecutor, TransportAddressConnector transportAddressConnector,
               Supplier<ExecutorService> executorServiceFactory) {
        super(settings);
        findPeersDelay = DISCOVERY_FIND_PEERS_INTERVAL_SETTING.get(settings);
        resolveTimeout = UnicastZenPing.DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT.get(settings);
        this.transportService = transportService;
        this.hostsProvider = hostsProvider;
        this.futureExecutor = futureExecutor;
        this.transportAddressConnector = transportAddressConnector;
        this.executorServiceFactory = executorServiceFactory;
    }

    public Iterable<DiscoveryNode> getFoundPeers() {
        synchronized (mutex) {
            return getActivePeerFinder().foundPeers.keySet();
        }
    }

    public void activate() {
        if (lifecycle.started() == false) {
            logger.debug("ignoring activation, not started");
            return;
        }

        // Must not hold lock when calling out to Legislator to avoid a lock cycle
        assert holdsLock() == false : "Peerfinder mutex is held in error";
        final DiscoveryNodes lastAcceptedNodes = getLastAcceptedNodes();

        synchronized (mutex) {
            assert peerFinder.isPresent() == false;
            peerFinder = Optional.of(new ActivePeerFinder());
            peerFinder.get().start(lastAcceptedNodes);
        }
    }

    // exposed to subclasses for testing
    protected final boolean holdsLock() {
        return Thread.holdsLock(mutex);
    }

    public void deactivate() {
        synchronized (mutex) {
            if (peerFinder.isPresent()) {
                assert peerFinder.get().running;
                peerFinder.get().stop();
                peerFinder = Optional.empty();
            }
        }
    }

    private ActivePeerFinder getActivePeerFinder() {
        assert holdsLock() : "Peerfinder mutex not held";
        final ActivePeerFinder activePeerFinder = this.peerFinder.get();
        assert activePeerFinder.running;
        return activePeerFinder;
    }

    public List<DiscoveryNode> handlePeersRequest(PeersRequest peersRequest) {
        synchronized (mutex) {
            assert peersRequest.getSourceNode().equals(getLocalNode()) == false;
            final ActivePeerFinder activePeerFinder = getActivePeerFinder();
            activePeerFinder.startProbe(peersRequest.getSourceNode().getAddress());
            peersRequest.getDiscoveryNodes().stream().map(DiscoveryNode::getAddress).forEach(activePeerFinder::startProbe);
            return new ArrayList<>(activePeerFinder.foundPeers.keySet());
        }
    }

    public boolean isActive() {
        synchronized (mutex) {
            if (peerFinder.isPresent()) {
                assert peerFinder.get().running;
                return true;
            } else {
                return false;
            }
        }
    }

    private DiscoveryNode getLocalNode() {
        // No synchronisation required
        final DiscoveryNode localNode = transportService.getLocalNode();
        assert localNode != null;
        return localNode;
    }

    @Override
    protected void doStart() {
        // No synchronisation required
        executorService.set(executorServiceFactory.get());
    }

    @Override
    protected void doStop() {
        // No synchronisation required
        ThreadPool.terminate(executorService.get(), 10, TimeUnit.SECONDS);
    }

    @Override
    protected void doClose() {
    }

    protected abstract DiscoveryNodes getLastAcceptedNodes();

    protected abstract void onMasterFoundByProbe(DiscoveryNode masterNode, long term);

    public interface TransportAddressConnector {
        /**
         * Identify the node at the given address and establish a full connection to it.
         *
         * @return The node to which we connected.
         */
        DiscoveryNode connectTo(TransportAddress transportAddress) throws IOException;
    }

    private class ActivePeerFinder {
        private boolean running;
        private final Map<DiscoveryNode, FoundPeer> foundPeers;
        private final Set<TransportAddress> probedAddresses = new HashSet<>();
        private final AtomicBoolean resolveInProgress = new AtomicBoolean();
        private final AtomicReference<List<TransportAddress>> lastConnectedAddresses = new AtomicReference<>(Collections.emptyList());
        private final Map<TransportAddress, DiscoveryNode> connectedNodes = newConcurrentMap();

        ActivePeerFinder() {
            foundPeers = newConcurrentMap();
            foundPeers.put(getLocalNode(), new FoundPeer(getLocalNode()));
        }

        void start(DiscoveryNodes lastAcceptedNodes) {
            assert holdsLock() : "PeerFinder mutex not held";
            assert running == false;
            running = true;
            handleWakeUpUnderLock(lastAcceptedNodes);
        }

        void stop() {
            assert holdsLock() : "PeerFinder mutex not held";
            assert running;
            running = false;
        }

        private void handleWakeUp() {
            // Must not hold lock when calling out to Legislator to avoid a lock cycle
            assert holdsLock() == false : "Peerfinder mutex is held in error";
            final DiscoveryNodes lastAcceptedNodes = getLastAcceptedNodes();
            synchronized (mutex) {
                handleWakeUpUnderLock(lastAcceptedNodes);
            }
        }

        private void handleWakeUpUnderLock(DiscoveryNodes lastAcceptedNodes) {
            assert holdsLock() : "PeerFinder mutex not held";

            if (running == false) {
                return;
            }

            for (final FoundPeer foundPeer : foundPeers.values()) {
                foundPeer.peersRequestedThisRound = false;
            }
            probedAddresses.clear();

            logger.trace("ActivePeerFinder#handleWakeUp(): probing found peers {}", foundPeers.keySet());
            foundPeers.keySet().forEach(this::startProbe);

            final ImmutableOpenMap<String, DiscoveryNode> masterNodes = lastAcceptedNodes.getMasterNodes();
            logger.trace("ActivePeerFinder#handleWakeUp(): probing nodes in cluster state {}", masterNodes);
            masterNodes.forEach(e -> startProbe(e.value));

            if (resolveInProgress.compareAndSet(false, true)) {
                executorService.get().execute(labelRunnable(() -> {
                    // No synchronisation required for most of this
                    List<TransportAddress> providedAddresses
                        = new ArrayList<>(hostsProvider.buildDynamicHosts((hosts, limitPortCounts)
                        -> UnicastZenPing.resolveHostsLists(executorService.get(), logger, hosts, limitPortCounts,
                        transportService, resolveTimeout)));

                    // localNode is excluded from buildDynamicNodes
                    providedAddresses.add(transportService.getLocalNode().getAddress());

                    lastConnectedAddresses.set(unmodifiableList(providedAddresses));

                    logger.trace("ActivePeerFinder#handleNextWakeUp(): probing resolved transport addresses {}", providedAddresses);
                    synchronized (mutex) {
                        providedAddresses.forEach(ActivePeerFinder.this::startProbe);
                    }

                    resolveInProgress.set(false);
                }, "PeerFinder resolving unicast hosts list"));
            }

            final List<TransportAddress> transportAddresses = lastConnectedAddresses.get();
            logger.trace("ActivePeerFinder#handleNextWakeUp(): probing supplied transport addresses {}", transportAddresses);
            transportAddresses.forEach(this::startProbe);

            futureExecutor.schedule(labelRunnable(this::handleWakeUp, "ActivePeerFinder::handleWakeUp"), findPeersDelay);
        }

        void startProbe(DiscoveryNode discoveryNode) {
            startProbe(discoveryNode.getAddress());
        }

        void startProbe(TransportAddress transportAddress) {
            assert holdsLock() : "PeerFinder mutex not held";
            if (running == false) {
                logger.trace("startProbe({}) not running", transportAddress);
                return;
            }

            if (transportAddress.equals(getLocalNode().getAddress())) {
                logger.trace("startProbe({}) not probing local node", transportAddress);
                return;
            }

            if (probedAddresses.add(transportAddress) == false) {
                logger.trace("startProbe({}) already probed this round", transportAddress);
                return;
            }

            final DiscoveryNode cachedNode = connectedNodes.get(transportAddress);
            if (cachedNode != null) {
                assert cachedNode.getAddress().equals(transportAddress);

                if (transportService.nodeConnected(cachedNode)) {
                    logger.trace("startProbe({}) found connected {}", transportAddress, cachedNode);
                    onProbeSuccess(cachedNode);
                    return;
                }

                logger.trace("startProbe({}) found disconnected {}, probing again", transportAddress, cachedNode);
                connectedNodes.remove(transportAddress, cachedNode);
            } else {
                logger.trace("startProbe({}) no cached node found, probing", transportAddress);
            }

            executorService.get().execute(new AbstractRunnable() {

                @Override
                protected void doRun() throws Exception {
                    // No synchronisation required - transportAddressConnector, connectedNodes, onProbeSuccess all threadsafe
                    final DiscoveryNode remoteNode = transportAddressConnector.connectTo(transportAddress);
                    connectedNodes.put(remoteNode.getAddress(), remoteNode);
                    onProbeSuccess(remoteNode);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug(() -> new ParameterizedMessage("Probing {} failed", transportAddress), e);
                }

                @Override
                public String toString() {
                    return "probe " + transportAddress;
                }
            });
        }

        private void onProbeSuccess(final DiscoveryNode discoveryNode) {
            // Called when we are fully connected to the given DiscoveryNode. Request its peers once per round, and probe them too.
            synchronized (mutex) {
                if (running == false) {
                    return;
                }

                if (discoveryNode.isMasterNode() == false) {
                    logger.trace("Ignoring non-master node {}", discoveryNode);
                    return;
                }

                final FoundPeer foundPeer = getFoundPeer(discoveryNode);

                if (foundPeer.peersRequestedThisRound) {
                    return;
                }
                foundPeer.peersRequestedThisRound = true;

                if (discoveryNode.equals(getLocalNode())) {
                    return;
                }

                List<DiscoveryNode> knownNodes = new ArrayList<>(foundPeers.keySet());

                transportService.sendRequest(discoveryNode, REQUEST_PEERS_ACTION_NAME,
                    new PeersRequest(getLocalNode(), new ArrayList<>(knownNodes)),
                    new TransportResponseHandler<PeersResponse>() {

                        @Override
                        public PeersResponse read(StreamInput in) throws IOException {
                            return new PeersResponse(in);
                        }

                        @Override
                        public void handleResponse(PeersResponse response) {
                            logger.trace("Received {} from {}", response, discoveryNode);
                            final boolean foundMasterNode;
                            synchronized (mutex) {
                                if (running == false) {
                                    return;
                                }

                                if (response.getMasterNode().isPresent()) {
                                    final DiscoveryNode masterNode = response.getMasterNode().get();
                                    if (masterNode.equals(discoveryNode)) {
                                        foundMasterNode = true;
                                    } else {
                                        foundMasterNode = false;
                                        startProbe(masterNode);
                                    }
                                } else {
                                    foundMasterNode = false;
                                    response.getDiscoveryNodes().stream().map(DiscoveryNode::getAddress)
                                        .forEach(ActivePeerFinder.this::startProbe);
                                }
                            }

                            if (foundMasterNode) {
                                // Must not hold lock when calling out to Legislator to avoid a lock cycle
                                assert holdsLock() == false : "Peerfinder mutex is held in error";
                                onMasterFoundByProbe(discoveryNode, response.getTerm());
                            }
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            logger.debug("PeersRequest failed", exp);
                        }

                        @Override
                        public String executor() {
                            return Names.GENERIC;
                        }
                    });
            }
        }

        private FoundPeer getFoundPeer(DiscoveryNode discoveryNode) {
            // no synchronisation required: computeIfAbsent is atomic as foundPeers is a newConcurrentMap()
            FoundPeer foundPeer = foundPeers.computeIfAbsent(discoveryNode, FoundPeer::new);
            assert foundPeer.discoveryNode.equals(discoveryNode);
            return foundPeer;
        }

        class FoundPeer {
            final DiscoveryNode discoveryNode;
            boolean peersRequestedThisRound;

            FoundPeer(DiscoveryNode discoveryNode) {
                this.discoveryNode = discoveryNode;
            }

            @Override
            public String toString() {
                return "FoundPeer{" +
                    "discoveryNode=" + discoveryNode +
                    ", peersRequestedThisRound=" + peersRequestedThisRound +
                    '}';
            }
        }
    }
}
