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
import org.elasticsearch.action.ActionListener;
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
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentSet;

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
    private volatile long currentTerm;

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

        transportService.registerRequestHandler(REQUEST_PEERS_ACTION_NAME, Names.GENERIC, false, false,
            PeersRequest::new,
            (request, channel, task) -> channel.sendResponse(handlePeersRequest(request)));
    }

    public Iterable<DiscoveryNode> getFoundPeers() {
        synchronized (mutex) {
            return getActivePeerFinder().connectedNodes.values();
        }
    }

    public void activate(final DiscoveryNodes lastAcceptedNodes) {
        if (lifecycle.started() == false) {
            logger.debug("ignoring activation, not started");
            return;
        }

        synchronized (mutex) {
            assert peerFinder.isPresent() == false;
            peerFinder = Optional.of(new ActivePeerFinder(lastAcceptedNodes));
            peerFinder.get().start();
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

    PeersResponse handlePeersRequest(PeersRequest peersRequest) {
        synchronized (mutex) {
            assert peersRequest.getSourceNode().equals(getLocalNode()) == false;
            if (isActive()) {
                final ActivePeerFinder activePeerFinder = getActivePeerFinder();
                activePeerFinder.startProbe(peersRequest.getSourceNode().getAddress());
                peersRequest.getKnownPeers().stream().map(DiscoveryNode::getAddress).forEach(activePeerFinder::startProbe);
                return new PeersResponse(Optional.empty(), new ArrayList<>(activePeerFinder.connectedNodes.values()), currentTerm);
            }
        }

        return onPeersRequestWhenInactive(peersRequest.getSourceNode());
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

    public void setCurrentTerm(long currentTerm) {
        // Volatile write, no synchronisation required
        this.currentTerm = currentTerm;
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

    /**
     * Called on receipt of a PeersResponse from a node that believes it's an active leader, which this node should therefore try and join.
     */
    protected abstract void onMasterFoundByProbe(DiscoveryNode masterNode, long term);

    /**
     * Called on receipt of a PeersRequest when there is no ActivePeerFinder, which mostly means that this node thinks there's an
     * active leader. However, there's no synchronisation around this call so it's possible that it's called if there's no active
     * leader, but we have not yet been activated. If so, this should respond indicating that there's no active leader, and no
     * known peers.
     *
     * @param sourceNode The sender of the PeersRequest
     */
    protected abstract PeersResponse onPeersRequestWhenInactive(DiscoveryNode sourceNode);

    public interface TransportAddressConnector {
        /**
         * Identify the node at the given address and establish a full connection to it.
         */
        void connectTo(TransportAddress transportAddress, ActionListener<DiscoveryNode> listener);
    }

    private class ActivePeerFinder {
        private final DiscoveryNodes lastAcceptedNodes;
        boolean running;
        private final AtomicBoolean resolveInProgress = new AtomicBoolean();
        private final AtomicReference<List<TransportAddress>> lastResolvedAddresses = new AtomicReference<>(Collections.emptyList());
        private final Set<TransportAddress> probedAddressesSinceLastWakeUp = new HashSet<>();
        private final Set<TransportAddress> inFlightProbes = newConcurrentSet();
        private final Map<TransportAddress, DiscoveryNode> connectedNodes = newConcurrentMap();
        final Set<DiscoveryNode> peersRequestRecipientsSinceLastWakeUp = new HashSet<>();

        ActivePeerFinder(DiscoveryNodes lastAcceptedNodes) {
            this.lastAcceptedNodes = lastAcceptedNodes;
        }

        void start() {
            assert holdsLock() : "PeerFinder mutex not held";
            assert running == false;
            running = true;
            handleWakeUpUnderLock();
        }

        void stop() {
            assert holdsLock() : "PeerFinder mutex not held";
            assert running;
            running = false;
        }

        private void handleWakeUp() {
            synchronized (mutex) {
                handleWakeUpUnderLock();
            }
        }

        private void handleWakeUpUnderLock() {
            assert holdsLock() : "PeerFinder mutex not held";

            if (running == false) {
                logger.trace("ActivePeerFinder#handleWakeUp(): not running");
                return;
            }

            probedAddressesSinceLastWakeUp.clear();
            peersRequestRecipientsSinceLastWakeUp.clear();

            logger.trace("ActivePeerFinder#handleWakeUp(): probing connected peers {}", connectedNodes.keySet());
            connectedNodes.keySet().forEach(this::startProbe);

            final ImmutableOpenMap<String, DiscoveryNode> masterNodes = lastAcceptedNodes.getMasterNodes();
            logger.trace("ActivePeerFinder#handleWakeUp(): probing nodes in cluster state {}", masterNodes);
            masterNodes.forEach(e -> startProbe(e.value));

            if (resolveInProgress.compareAndSet(false, true)) {
                transportService.getThreadPool().generic().execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                    }

                    @Override
                    protected void doRun() {
                        // No synchronisation required for most of this
                        List<TransportAddress> providedAddresses
                            = new ArrayList<>(hostsProvider.buildDynamicHosts((hosts, limitPortCounts)
                            -> UnicastZenPing.resolveHostsLists(executorService.get(), logger, hosts, limitPortCounts,
                            transportService, resolveTimeout)));

                        lastResolvedAddresses.set(unmodifiableList(providedAddresses));

                        logger.trace("ActivePeerFinder#handleNextWakeUp(): probing resolved transport addresses {}", providedAddresses);
                        synchronized (mutex) {
                            providedAddresses.forEach(ActivePeerFinder.this::startProbe);
                        }
                    }

                    @Override
                    public void onAfter() {
                        super.onAfter();
                        resolveInProgress.set(false);
                    }

                    @Override
                    public String toString() {
                        return "PeerFinder resolving unicast hosts list";
                    }
                });
            }

            final List<TransportAddress> transportAddresses = lastResolvedAddresses.get();
            logger.trace("ActivePeerFinder#handleNextWakeUp(): probing supplied transport addresses {}", transportAddresses);
            transportAddresses.forEach(this::startProbe);

            futureExecutor.schedule(new Runnable() {
                @Override
                public void run() {
                    handleWakeUp();
                }

                @Override
                public String toString() {
                    return "ActivePeerFinder::handleWakeUp";
                }
            }, findPeersDelay);
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

            if (probedAddressesSinceLastWakeUp.add(transportAddress) == false) {
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

            if (inFlightProbes.add(transportAddress)) {
                logger.trace("startProbe({}) starting new probe", transportAddress);
                executorService.get().execute(new AbstractRunnable() {
                    @Override
                    protected void doRun() {
                        // No synchronisation required - transportAddressConnector is threadsafe
                        transportAddressConnector.connectTo(transportAddress, new ActionListener<DiscoveryNode>() {
                            @Override
                            public void onResponse(DiscoveryNode remoteNode) {
                                // No synchronisation required - connectedNodes, inFlightProbes, onProbeSuccess all threadsafe
                                logger.trace("startProbe({}) found {}", transportAddress, remoteNode);
                                final boolean removed = inFlightProbes.remove(transportAddress);
                                assert removed;
                                if (remoteNode.isMasterNode()) {
                                    connectedNodes.put(remoteNode.getAddress(), remoteNode);
                                    onProbeSuccess(remoteNode);
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                // No synchronisation required - inFlightProbes is threadsafe
                                logger.debug(() -> new ParameterizedMessage("startProbe({}) failed", transportAddress), e);
                                final boolean removed = inFlightProbes.remove(transportAddress);
                                assert removed;
                            }
                        });
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // No synchronisation required - inFlightProbes is threadsafe
                        logger.debug(() -> new ParameterizedMessage("startProbe({}) failed", transportAddress), e);
                        final boolean removed = inFlightProbes.remove(transportAddress);
                        assert removed;
                    }

                    @Override
                    public String toString() {
                        return "probe " + transportAddress;
                    }
                });
            } else {
                logger.trace("startProbe({}) already probing", transportAddress);
            }
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

                if (discoveryNode.equals(getLocalNode())) {
                    return;
                }

                if (peersRequestRecipientsSinceLastWakeUp.add(discoveryNode) == false) {
                    return;
                }

                List<DiscoveryNode> knownNodes = new ArrayList<>(connectedNodes.values());

                transportService.sendRequest(discoveryNode, REQUEST_PEERS_ACTION_NAME,
                    new PeersRequest(getLocalNode(), knownNodes),
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
                                    response.getKnownPeers().stream().map(DiscoveryNode::getAddress)
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
    }
}
