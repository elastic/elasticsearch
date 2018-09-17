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

package org.elasticsearch.discovery;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.coordination.PeersResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

public abstract class PeerFinder extends AbstractComponent {

    public static final String REQUEST_PEERS_ACTION_NAME = "internal:discovery/request_peers";

    // the time between attempts to find all peers
    public static final Setting<TimeValue> DISCOVERY_FIND_PEERS_INTERVAL_SETTING =
        Setting.timeSetting("discovery.find_peers_interval",
            TimeValue.timeValueMillis(1000), TimeValue.timeValueMillis(1), Setting.Property.NodeScope);

    private final TimeValue findPeersDelay;

    private final Object mutex = new Object();
    private final TransportService transportService;
    private final TransportAddressConnector transportAddressConnector;
    private final ConfiguredHostsResolver configuredHostsResolver;

    private volatile long currentTerm;
    private boolean active;
    private DiscoveryNodes lastAcceptedNodes;
    private final Map<TransportAddress, Peer> peersByAddress = newConcurrentMap();
    private Optional<DiscoveryNode> leader = Optional.empty();

    public PeerFinder(Settings settings, TransportService transportService, TransportAddressConnector transportAddressConnector,
                      ConfiguredHostsResolver configuredHostsResolver) {
        super(settings);
        findPeersDelay = DISCOVERY_FIND_PEERS_INTERVAL_SETTING.get(settings);
        this.transportService = transportService;
        this.transportAddressConnector = transportAddressConnector;
        this.configuredHostsResolver = configuredHostsResolver;

        transportService.registerRequestHandler(REQUEST_PEERS_ACTION_NAME, Names.GENERIC, false, false,
            PeersRequest::new,
            (request, channel, task) -> channel.sendResponse(handlePeersRequest(request)));
    }

    public void activate(final DiscoveryNodes lastAcceptedNodes) {
        logger.trace("activating with {}", lastAcceptedNodes);

        synchronized (mutex) {
            assert assertInactiveWithNoKnownPeers();
            active = true;
            this.lastAcceptedNodes = lastAcceptedNodes;
            leader = Optional.empty();
            handleWakeUp(); // return value discarded: there are no known peers, so none can be disconnected
        }

        onFoundPeersUpdated(); // trigger a check for a quorum already
    }

    public void deactivate(DiscoveryNode leader) {
        final boolean peersRemoved;
        synchronized (mutex) {
            logger.trace("deactivating and setting leader to {}", leader);
            active = false;
            peersRemoved = handleWakeUp();
            this.leader = Optional.of(leader);
            assert assertInactiveWithNoKnownPeers();
        }
        if (peersRemoved) {
            onFoundPeersUpdated();
        }
    }

    // exposed to subclasses for testing
    protected final boolean holdsLock() {
        return Thread.holdsLock(mutex);
    }

    private boolean assertInactiveWithNoKnownPeers() {
        assert active == false;
        assert peersByAddress.isEmpty() : peersByAddress.keySet();
        return true;
    }

    PeersResponse handlePeersRequest(PeersRequest peersRequest) {
        synchronized (mutex) {
            assert peersRequest.getSourceNode().equals(getLocalNode()) == false;
            final List<DiscoveryNode> knownPeers;
            if (active) {
                assert leader.isPresent() == false : leader;
                startProbe(peersRequest.getSourceNode().getAddress());
                peersRequest.getKnownPeers().stream().map(DiscoveryNode::getAddress).forEach(this::startProbe);
                knownPeers = getFoundPeersUnderLock();
            } else {
                assert leader.isPresent();
                knownPeers = Collections.emptyList();
            }
            return new PeersResponse(leader, knownPeers, currentTerm);
        }
    }

    // exposed for checking invariant in o.e.c.c.Coordinator (public since this is a different package)
    public Optional<DiscoveryNode> getLeader() {
        synchronized (mutex) {
            return leader;
        }
    }

    public void setCurrentTerm(long currentTerm) {
        this.currentTerm = currentTerm;
    }

    private DiscoveryNode getLocalNode() {
        final DiscoveryNode localNode = transportService.getLocalNode();
        assert localNode != null;
        return localNode;
    }

    /**
     * Invoked on receipt of a PeersResponse from a node that believes it's an active leader, which this node should therefore try and join.
     * Note that invocations of this method are not synchronised. By the time it is called we may have been deactivated.
     */
    protected abstract void onActiveMasterFound(DiscoveryNode masterNode, long term);

    /**
     * Invoked when the set of found peers changes. Note that invocations of this method are not fully synchronised, so we only guarantee
     * that the change to the set of found peers happens before this method is invoked. If there are multiple concurrent changes then there
     * will be multiple concurrent invocations of this method, with no guarantee as to their order. For this reason we do not pass the
     * updated set of peers as an argument to this method, leaving it to the implementation to call getFoundPeers() with appropriate
     * synchronisation to avoid lost updates. Also, by the time this method is invoked we may have been deactivated.
     */
    protected abstract void onFoundPeersUpdated();

    public interface TransportAddressConnector {
        /**
         * Identify the node at the given address and, if it is a master node and not the local node then establish a full connection to it.
         */
        void connectToRemoteMasterNode(TransportAddress transportAddress, ActionListener<DiscoveryNode> listener);
    }

    public interface ConfiguredHostsResolver {
        /**
         * Attempt to resolve the configured unicast hosts list to a list of transport addresses.
         *
         * @param consumer Consumer for the resolved list. May not be called if an error occurs or if another resolution attempt is in
         *                 progress.
         */
        void resolveConfiguredHosts(Consumer<List<TransportAddress>> consumer);
    }

    public Iterable<DiscoveryNode> getFoundPeers() {
        synchronized (mutex) {
            return getFoundPeersUnderLock();
        }
    }

    private List<DiscoveryNode> getFoundPeersUnderLock() {
        assert holdsLock() : "PeerFinder mutex not held";
        return peersByAddress.values().stream().map(Peer::getDiscoveryNode).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private Peer createConnectingPeer(TransportAddress transportAddress) {
        Peer peer = new Peer(transportAddress);
        peer.establishConnection();
        return peer;
    }

    /**
     * @return whether any peers were removed due to disconnection
     */
    private boolean handleWakeUp() {
        assert holdsLock() : "PeerFinder mutex not held";

        boolean peersRemoved = false;

        for (final Peer peer : peersByAddress.values()) {
            peersRemoved = peer.handleWakeUp() || peersRemoved; // care: avoid short-circuiting, each peer needs waking up
        }

        if (active == false) {
            logger.trace("not active");
            return peersRemoved;
        }

        logger.trace("probing master nodes from cluster state: {}", lastAcceptedNodes);
        for (ObjectCursor<DiscoveryNode> discoveryNodeObjectCursor : lastAcceptedNodes.getMasterNodes().values()) {
            startProbe(discoveryNodeObjectCursor.value.getAddress());
        }

        configuredHostsResolver.resolveConfiguredHosts(providedAddresses -> {
            synchronized (mutex) {
                logger.trace("probing resolved transport addresses {}", providedAddresses);
                providedAddresses.forEach(this::startProbe);
            }
        });

        transportService.getThreadPool().schedule(findPeersDelay, Names.GENERIC, new AbstractRunnable() {
            @Override
            public boolean isForceExecution() {
                return true;
            }

            @Override
            public void onFailure(Exception e) {
                assert false : e;
                logger.debug("unexpected exception in wakeup", e);
            }

            @Override
            protected void doRun() {
                synchronized (mutex) {
                    if (handleWakeUp() == false) {
                        return;
                    }
                }
                onFoundPeersUpdated();
            }

            @Override
            public String toString() {
                return "PeerFinder handling wakeup";
            }
        });

        return peersRemoved;
    }

    private void startProbe(TransportAddress transportAddress) {
        assert holdsLock() : "PeerFinder mutex not held";
        if (active == false) {
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

        @Nullable
        DiscoveryNode getDiscoveryNode() {
            return discoveryNode.get();
        }

        boolean handleWakeUp() {
            assert holdsLock() : "PeerFinder mutex not held";

            if (active == false) {
                removePeer();
                return true;
            }

            final DiscoveryNode discoveryNode = getDiscoveryNode();
            // may be null if connection not yet established

            if (discoveryNode != null) {
                if (transportService.nodeConnected(discoveryNode)) {
                    if (peersRequestInFlight == false) {
                        requestPeers();
                    }
                } else {
                    logger.trace("{} no longer connected", this);
                    removePeer();
                    return true;
                }
            }

            return false;
        }

        void establishConnection() {
            assert holdsLock() : "PeerFinder mutex not held";
            assert getDiscoveryNode() == null : "unexpectedly connected to " + getDiscoveryNode();
            assert active;

            logger.trace("{} attempting connection", this);
            transportAddressConnector.connectToRemoteMasterNode(transportAddress, new ActionListener<DiscoveryNode>() {
                @Override
                public void onResponse(DiscoveryNode remoteNode) {
                    assert remoteNode.isMasterNode() : remoteNode + " is not master-eligible";
                    assert remoteNode.equals(getLocalNode()) == false : remoteNode + " is the local node";
                    synchronized (mutex) {
                        if (active == false) {
                            return;
                        }

                        assert discoveryNode.get() == null : "discoveryNode unexpectedly already set to " + discoveryNode.get();
                        discoveryNode.set(remoteNode);
                        requestPeers();
                    }

                    assert holdsLock() == false : "PeerFinder mutex is held in error";
                    onFoundPeersUpdated();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug(() -> new ParameterizedMessage("{} connection failed", Peer.this), e);
                    removePeer();
                }
            });
        }

        private void removePeer() {
            final Peer removed = peersByAddress.remove(transportAddress);
            assert removed == Peer.this;
        }

        private void requestPeers() {
            assert holdsLock() : "PeerFinder mutex not held";
            assert peersRequestInFlight == false : "PeersRequest already in flight";
            assert active;

            final DiscoveryNode discoveryNode = getDiscoveryNode();
            assert discoveryNode != null : "cannot request peers without first connecting";

            logger.trace("{} requesting peers", this);
            peersRequestInFlight = true;

            List<DiscoveryNode> knownNodes = getFoundPeersUnderLock();

            transportService.sendRequest(discoveryNode, REQUEST_PEERS_ACTION_NAME,
                new PeersRequest(getLocalNode(), knownNodes),
                new TransportResponseHandler<PeersResponse>() {

                    @Override
                    public PeersResponse read(StreamInput in) throws IOException {
                        return new PeersResponse(in);
                    }

                    @Override
                    public void handleResponse(PeersResponse response) {
                        logger.trace("{} received {}", Peer.this, response);
                        synchronized (mutex) {
                            if (active == false) {
                                return;
                            }

                            peersRequestInFlight = false;

                            response.getMasterNode().map(DiscoveryNode::getAddress).ifPresent(PeerFinder.this::startProbe);
                            response.getKnownPeers().stream().map(DiscoveryNode::getAddress).forEach(PeerFinder.this::startProbe);
                        }

                        if (response.getMasterNode().equals(Optional.of(discoveryNode))) {
                            // Must not hold lock here to avoid deadlock
                            assert holdsLock() == false : "PeerFinder mutex is held in error";
                            onActiveMasterFound(discoveryNode, response.getTerm());
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        peersRequestInFlight = false;
                        logger.debug(new ParameterizedMessage("{} peers request failed", this), exp);
                    }

                    @Override
                    public String executor() {
                        return Names.GENERIC;
                    }
                });
        }

        @Override
        public String toString() {
            return "Peer{" +
                "transportAddress=" + transportAddress +
                ", discoveryNode=" + discoveryNode.get() +
                ", peersRequestInFlight=" + peersRequestInFlight +
                '}';
        }
    }
}
