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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

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
            return getActivePeerFinder().getKnownPeers();
        }
    }

    public void activate(final DiscoveryNodes lastAcceptedNodes) {
        if (lifecycle.started() == false) {
            logger.debug("ignoring activation, not started");
            return;
        }

        logger.trace("activating PeerFinder {}", lastAcceptedNodes);

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
                logger.trace("deactivating PeerFinder");
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
                return new PeersResponse(Optional.empty(), activePeerFinder.getKnownPeers(), currentTerm);
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
         * Identify the node at the given address and, if it is a master node and not the local node then establish a full connection to it.
         */
        void connectToRemoteMasterNode(TransportAddress transportAddress, ActionListener<DiscoveryNode> listener);
    }

    private class ActivePeerFinder {
        private final DiscoveryNodes lastAcceptedNodes;
        boolean running;
        private final AtomicBoolean resolveInProgress = new AtomicBoolean();
        private final Map<TransportAddress, Peer> peersByAddress = newConcurrentMap();

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

        List<DiscoveryNode> getKnownPeers() {
            assert holdsLock() : "PeerFinder mutex not held";
            List<DiscoveryNode> knownPeers = new ArrayList<>(peersByAddress.size());
            for(final Peer peer : peersByAddress.values()) {
                DiscoveryNode peerNode = peer.getDiscoveryNode();
                if (peerNode != null) {
                    knownPeers.add(peerNode);
                }
            }
            return knownPeers;
        }

        private Peer createConnectingPeer(TransportAddress transportAddress) {
            Peer peer = new Peer(transportAddress);
            peer.establishConnection();
            return peer;
        }

        private void handleWakeUpUnderLock() {
            assert holdsLock() : "PeerFinder mutex not held";

            if (running == false) {
                logger.trace("ActivePeerFinder#handleWakeUp(): not running");
                return;
            }

            for (final Peer peer : peersByAddress.values()) {
                peer.handleWakeUp();
            }

            for (ObjectCursor<DiscoveryNode> discoveryNodeObjectCursor : lastAcceptedNodes.getMasterNodes().values()) {
                startProbe(discoveryNodeObjectCursor.value.getAddress());
            }

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

            peersByAddress.computeIfAbsent(transportAddress, this::createConnectingPeer);
        }

        private class Peer {
            private final TransportAddress transportAddress;
            private SetOnce<DiscoveryNode> discoveryNode = new SetOnce<>();
            private volatile boolean peersRequestInFlight;

            Peer(TransportAddress transportAddress) {
                this.transportAddress = transportAddress;
            }

            DiscoveryNode getDiscoveryNode() {
                // No synchronisation required
                return discoveryNode.get();
            }

            void handleWakeUp() {
                assert holdsLock() : "PeerFinder mutex not held";

                if (running == false) {
                    return;
                }

                final DiscoveryNode discoveryNode = this.discoveryNode.get();
                // may be null if connection not yet established

                if (discoveryNode != null) {
                    if (transportService.nodeConnected(discoveryNode)) {
                        if (peersRequestInFlight == false) {
                            requestPeers(discoveryNode);
                        }
                    } else {
                        logger.trace("{} no longer connected to {}", this, discoveryNode);
                        removePeer();
                    }
                }
            }

            void establishConnection() {
                assert holdsLock() : "PeerFinder mutex not held";
                assert getDiscoveryNode() == null : "unexpectedly connected to " + getDiscoveryNode();
                assert running;

                logger.trace("{} attempting connection", this);
                transportAddressConnector.connectToRemoteMasterNode(transportAddress, new ActionListener<DiscoveryNode>() {
                    @Override
                    public void onResponse(DiscoveryNode remoteNode) {
                        assert remoteNode.isMasterNode() : remoteNode + " is not master-eligible";
                        assert remoteNode.equals(getLocalNode()) == false : remoteNode + " is the local node";
                        synchronized (mutex) {
                            if (running) {
                                discoveryNode.set(remoteNode);
                                requestPeers(remoteNode);
                            }
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // No synchronisation required - removePeer is threadsafe
                        logger.debug(() -> new ParameterizedMessage("{} connection failed", Peer.this), e);
                        removePeer();
                    }
                });
            }

            private void removePeer() {
                // No synchronisation required - peersByAddress is threadsafe
                final Peer removed = peersByAddress.remove(transportAddress);
                assert removed == Peer.this;
            }

            private void requestPeers(final DiscoveryNode discoveryNode) {
                assert holdsLock() : "PeerFinder mutex not held";
                assert peersRequestInFlight == false : "PeersRequest already in flight";
                assert running;
                assert discoveryNode.equals(this.discoveryNode.get());

                logger.trace("{} requesting peers from {}", this, discoveryNode);
                peersRequestInFlight = true;

                List<DiscoveryNode> knownNodes = getKnownPeers();

                transportService.sendRequest(discoveryNode, REQUEST_PEERS_ACTION_NAME,
                    new PeersRequest(getLocalNode(), knownNodes),
                    new TransportResponseHandler<PeersResponse>() {

                        @Override
                        public PeersResponse read(StreamInput in) throws IOException {
                            return new PeersResponse(in);
                        }

                        @Override
                        public void handleResponse(PeersResponse response) {
                            logger.trace("{} received {} from {}", this, response, discoveryNode);
                            final boolean foundMasterNode;
                            synchronized (mutex) {
                                if (running == false) {
                                    return;
                                }

                                peersRequestInFlight = false;

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
                                // Must not hold lock here to avoid deadlock
                                assert holdsLock() == false : "PeerFinder mutex is held in error";
                                onMasterFoundByProbe(discoveryNode, response.getTerm());
                            }
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            peersRequestInFlight = false; // volatile write needs no further synchronisation
                            logger.debug("PeersRequest failed", exp);
                        }

                        @Override
                        public String executor() {
                            return Names.GENERIC;
                        }
                    });
            }

            @Override
            public String toString() {
                return "Peer{" + transportAddress + " peersRequestInFlight=" + peersRequestInFlight + "}";
            }
        }
    }
}
