/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.coordination.PeersResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.elasticsearch.core.Strings.format;

public abstract class PeerFinder {

    private static final Logger logger = LogManager.getLogger(PeerFinder.class);

    public static final String REQUEST_PEERS_ACTION_NAME = "internal:discovery/request_peers";

    // the time between attempts to find all peers
    public static final Setting<TimeValue> DISCOVERY_FIND_PEERS_INTERVAL_SETTING = Setting.timeSetting(
        "discovery.find_peers_interval",
        TimeValue.timeValueMillis(1000),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> DISCOVERY_REQUEST_PEERS_TIMEOUT_SETTING = Setting.timeSetting(
        "discovery.request_peers_timeout",
        TimeValue.timeValueMillis(3000),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );

    // We do not log connection failures immediately: some failures are expected, especially if the hosts list isn't perfectly up-to-date
    // or contains some unnecessary junk. However if the node cannot find a master for an extended period of time then it is helpful to
    // users to describe in more detail why we cannot connect to the remote nodes. This setting defines how long we wait without discovering
    // the master before we start to emit more verbose logs.
    public static final Setting<TimeValue> VERBOSITY_INCREASE_TIMEOUT_SETTING = Setting.timeSetting(
        "discovery.find_peers_warning_timeout",
        TimeValue.timeValueMinutes(3),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );

    private final TimeValue findPeersInterval;
    private final TimeValue requestPeersTimeout;
    private final TimeValue verbosityIncreaseTimeout;

    private final Object mutex = new Object();
    private final TransportService transportService;
    private final TransportAddressConnector transportAddressConnector;
    private final ConfiguredHostsResolver configuredHostsResolver;

    private volatile long currentTerm;
    private boolean active;
    private long activatedAtMillis;
    private DiscoveryNodes lastAcceptedNodes;
    private final Map<TransportAddress, Peer> peersByAddress = new LinkedHashMap<>();
    private Optional<DiscoveryNode> leader = Optional.empty();
    private volatile List<TransportAddress> lastResolvedAddresses = emptyList();

    public PeerFinder(
        Settings settings,
        TransportService transportService,
        TransportAddressConnector transportAddressConnector,
        ConfiguredHostsResolver configuredHostsResolver
    ) {
        findPeersInterval = DISCOVERY_FIND_PEERS_INTERVAL_SETTING.get(settings);
        requestPeersTimeout = DISCOVERY_REQUEST_PEERS_TIMEOUT_SETTING.get(settings);
        verbosityIncreaseTimeout = VERBOSITY_INCREASE_TIMEOUT_SETTING.get(settings);
        this.transportService = transportService;
        this.transportAddressConnector = transportAddressConnector;
        this.configuredHostsResolver = configuredHostsResolver;

        transportService.registerRequestHandler(
            REQUEST_PEERS_ACTION_NAME,
            Names.CLUSTER_COORDINATION,
            false,
            false,
            PeersRequest::new,
            (request, channel, task) -> channel.sendResponse(handlePeersRequest(request))
        );
    }

    public void activate(final DiscoveryNodes lastAcceptedNodes) {
        logger.trace("activating with {}", lastAcceptedNodes);

        synchronized (mutex) {
            assert assertInactiveWithNoKnownPeers();
            active = true;
            activatedAtMillis = transportService.getThreadPool().relativeTimeInMillis();
            this.lastAcceptedNodes = lastAcceptedNodes;
            leader = Optional.empty();
            handleWakeUp(); // return value discarded: there are no known peers, so none can be disconnected
        }

        onFoundPeersUpdated(); // trigger a check for a quorum already
    }

    public void deactivate(DiscoveryNode leader) {
        final boolean peersRemoved;
        final Collection<Releasable> connectionReferences;
        synchronized (mutex) {
            logger.trace("deactivating and setting leader to {}", leader);
            active = false;
            connectionReferences = new ArrayList<>(peersByAddress.size());
            for (Peer peer : peersByAddress.values()) {
                connectionReferences.add(peer.getConnectionReference());
            }
            peersRemoved = handleWakeUp();
            this.leader = Optional.of(leader);
            assert assertInactiveWithNoKnownPeers();
        }
        if (peersRemoved) {
            onFoundPeersUpdated();
        }

        // Discovery is over, we're joining a cluster, so we can release all the connections that were being used for discovery. We haven't
        // finished joining/forming the cluster yet, but if we're joining an existing master then the join will hold open the connection
        // it's using and if we're becoming the master then join validation will hold open the connections to the joining peers; this set of
        // peers is a quorum so that's good enough.
        //
        // Note however that this might still close connections to other master-eligible nodes that we discovered but which aren't currently
        // involved in joining: either they're not the master we're joining or else we're becoming the master but they didn't try and join
        // us yet. It's a pretty safe bet that we'll want to have connections to these nodes in the near future: either they're already in
        // the cluster or else they will discover we're the master and join us straight away. In theory we could keep these discovery
        // connections open for a while rather than closing them here and then reopening them again, but in practice it's so much simpler to
        // forget about them for now.
        //
        // Note also that the NodeConnectionsService is still maintaining connections to the nodes in the last-applied cluster state, so
        // this will only close connections to nodes that we didn't know about beforehand. In most cases that's because we only just started
        // and haven't applied any cluster states at all yet. This won't cause any connection disruption during a typical master failover.
        assert peersRemoved || connectionReferences.isEmpty() : connectionReferences;
        Releasables.close(connectionReferences);
    }

    // exposed to subclasses for testing
    protected final boolean holdsLock() {
        return Thread.holdsLock(mutex);
    }

    private boolean assertInactiveWithNoKnownPeers() {
        assert holdsLock() : "PeerFinder mutex not held";
        assert active == false;
        assert peersByAddress.isEmpty() : peersByAddress.keySet();
        return true;
    }

    PeersResponse handlePeersRequest(PeersRequest peersRequest) {
        final Collection<DiscoveryNode> knownPeers;
        final Optional<DiscoveryNode> leader;
        final long currentTerm;
        synchronized (mutex) {
            assert peersRequest.getSourceNode().equals(getLocalNode()) == false;
            leader = this.leader;
            currentTerm = this.currentTerm;
            if (active) {
                assert leader.isPresent() == false : leader;
                if (peersRequest.getSourceNode().isMasterNode()) {
                    startProbe(peersRequest.getSourceNode().getAddress());
                }
                peersRequest.getKnownPeers().stream().map(DiscoveryNode::getAddress).forEach(this::startProbe);
                knownPeers = getFoundPeersUnderLock();
            } else {
                assert leader.isPresent() || lastAcceptedNodes == null;
                knownPeers = emptyList();
            }
        }
        return new PeersResponse(leader, List.copyOf(knownPeers), currentTerm);
    }

    // exposed for checking invariant in o.e.c.c.Coordinator (public since this is a different package)
    public Optional<DiscoveryNode> getLeader() {
        synchronized (mutex) {
            return leader;
        }
    }

    // exposed for checking invariant in o.e.c.c.Coordinator (public since this is a different package)
    public long getCurrentTerm() {
        return currentTerm;
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

    public List<TransportAddress> getLastResolvedAddresses() {
        return lastResolvedAddresses;
    }

    public Iterable<DiscoveryNode> getFoundPeers() {
        synchronized (mutex) {
            return getFoundPeersUnderLock();
        }
    }

    private Collection<DiscoveryNode> getFoundPeersUnderLock() {
        assert holdsLock() : "PeerFinder mutex not held";
        Set<DiscoveryNode> peers = Sets.newHashSetWithExpectedSize(peersByAddress.size());
        for (Peer peer : peersByAddress.values()) {
            DiscoveryNode discoveryNode = peer.getDiscoveryNode();
            if (discoveryNode != null) {
                peers.add(discoveryNode);
            }
        }
        return peers;
    }

    /**
     * @return whether any peers were removed due to disconnection
     */
    private boolean handleWakeUp() {
        assert holdsLock() : "PeerFinder mutex not held";

        final boolean peersRemoved = peersByAddress.values().removeIf(Peer::handleWakeUp);

        if (active == false) {
            logger.trace("not active");
            return peersRemoved;
        }

        logger.trace("probing master nodes from cluster state: {}", lastAcceptedNodes);
        for (DiscoveryNode discoveryNode : lastAcceptedNodes.getMasterNodes().values()) {
            startProbe(discoveryNode.getAddress());
        }

        configuredHostsResolver.resolveConfiguredHosts(providedAddresses -> {
            synchronized (mutex) {
                lastResolvedAddresses = providedAddresses;
                logger.trace("probing resolved transport addresses {}", providedAddresses);
                providedAddresses.forEach(this::startProbe);
            }
        });

        transportService.getThreadPool().scheduleUnlessShuttingDown(findPeersInterval, Names.CLUSTER_COORDINATION, new Runnable() {
            @Override
            public void run() {
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

    protected void startProbe(TransportAddress transportAddress) {
        assert holdsLock() : "PeerFinder mutex not held";
        if (active == false) {
            logger.trace("startProbe({}) not running", transportAddress);
            return;
        }

        if (transportAddress.equals(getLocalNode().getAddress())) {
            logger.trace("startProbe({}) not probing local node", transportAddress);
            return;
        }

        if (peersByAddress.containsKey(transportAddress) == false) {
            final Peer peer = new Peer(transportAddress);
            peersByAddress.put(transportAddress, peer);
            peer.establishConnection();
        }
    }

    private class Peer {
        private final TransportAddress transportAddress;
        private final SetOnce<ProbeConnectionResult> probeConnectionResult = new SetOnce<>();
        private volatile boolean peersRequestInFlight;

        Peer(TransportAddress transportAddress) {
            this.transportAddress = transportAddress;
        }

        @Nullable
        DiscoveryNode getDiscoveryNode() {
            return Optional.ofNullable(probeConnectionResult.get()).map(ProbeConnectionResult::getDiscoveryNode).orElse(null);
        }

        private boolean isActive() {
            return active && peersByAddress.get(transportAddress) == this;
        }

        boolean handleWakeUp() {
            assert holdsLock() : "PeerFinder mutex not held";

            if (isActive() == false) {
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
                    return true;
                }
            }

            return false;
        }

        void establishConnection() {
            assert holdsLock() : "PeerFinder mutex not held";
            assert getDiscoveryNode() == null : "unexpectedly connected to " + getDiscoveryNode();
            assert isActive();

            final boolean verboseFailureLogging = transportService.getThreadPool().relativeTimeInMillis()
                - activatedAtMillis > verbosityIncreaseTimeout.millis();

            logger.trace("{} attempting connection", this);
            transportAddressConnector.connectToRemoteMasterNode(transportAddress, new ActionListener<ProbeConnectionResult>() {
                @Override
                public void onResponse(ProbeConnectionResult connectResult) {
                    assert holdsLock() == false : "PeerFinder mutex is held in error";
                    final DiscoveryNode remoteNode = connectResult.getDiscoveryNode();
                    assert remoteNode.isMasterNode() : remoteNode + " is not master-eligible";
                    assert remoteNode.equals(getLocalNode()) == false : remoteNode + " is the local node";
                    boolean retainConnection = false;
                    try {
                        synchronized (mutex) {
                            if (isActive() == false) {
                                return;
                            }

                            assert probeConnectionResult.get() == null
                                : "connection result unexpectedly already set to " + probeConnectionResult.get();
                            probeConnectionResult.set(connectResult);

                            requestPeers();
                        }

                        onFoundPeersUpdated();

                        retainConnection = true;
                    } finally {
                        if (retainConnection == false) {
                            Releasables.close(connectResult);
                        }
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (verboseFailureLogging) {
                        if (logger.isDebugEnabled()) {
                            // log message at level WARN, but since DEBUG logging is enabled we include the full stack trace
                            logger.warn(() -> format("%s discovery result", Peer.this), e);
                        } else {
                            final StringBuilder messageBuilder = new StringBuilder();
                            Throwable cause = e;
                            while (cause != null && messageBuilder.length() <= 1024) {
                                messageBuilder.append(": ").append(cause.getMessage());
                                cause = cause.getCause();
                            }
                            final String message = messageBuilder.length() < 1024
                                ? messageBuilder.toString()
                                : (messageBuilder.substring(0, 1023) + "...");
                            logger.warn("{} discovery result{}", Peer.this, message);
                        }
                    } else {
                        logger.debug(() -> format("%s discovery result", Peer.this), e);
                    }
                    synchronized (mutex) {
                        assert probeConnectionResult.get() == null
                            : "discoveryNode unexpectedly already set to " + probeConnectionResult.get();
                        if (isActive()) {
                            peersByAddress.remove(transportAddress);
                        } // else this Peer has been superseded by a different instance which should be left in place
                    }
                }
            });
        }

        private void requestPeers() {
            assert holdsLock() : "PeerFinder mutex not held";
            assert peersRequestInFlight == false : "PeersRequest already in flight";
            assert isActive();

            final DiscoveryNode discoveryNode = getDiscoveryNode();
            assert discoveryNode != null : "cannot request peers without first connecting";

            if (discoveryNode.equals(getLocalNode())) {
                logger.trace("{} not requesting peers from local node", this);
                return;
            }

            logger.trace("{} requesting peers", this);
            peersRequestInFlight = true;

            final List<DiscoveryNode> knownNodes = List.copyOf(getFoundPeersUnderLock());

            final TransportResponseHandler<PeersResponse> peersResponseHandler = new TransportResponseHandler<>() {

                @Override
                public PeersResponse read(StreamInput in) throws IOException {
                    return new PeersResponse(in);
                }

                @Override
                public void handleResponse(PeersResponse response) {
                    logger.trace("{} received {}", Peer.this, response);
                    synchronized (mutex) {
                        if (isActive() == false) {
                            return;
                        }

                        peersRequestInFlight = false;

                        response.getMasterNode().ifPresent(node -> startProbe(node.getAddress()));
                        for (DiscoveryNode node : response.getKnownPeers()) {
                            startProbe(node.getAddress());
                        }
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
                    logger.warn(() -> format("%s peers request failed", Peer.this), exp);
                }

                @Override
                public String executor() {
                    return Names.CLUSTER_COORDINATION;
                }
            };
            transportService.sendRequest(
                discoveryNode,
                REQUEST_PEERS_ACTION_NAME,
                new PeersRequest(getLocalNode(), knownNodes),
                TransportRequestOptions.timeout(requestPeersTimeout),
                peersResponseHandler
            );
        }

        @Nullable
        Releasable getConnectionReference() {
            assert holdsLock() : "PeerFinder mutex not held";
            return probeConnectionResult.get();
        }

        @Override
        public String toString() {
            return "address [" + transportAddress + "], node [" + getDiscoveryNode() + "], requesting [" + peersRequestInFlight + "]";
        }
    }
}
