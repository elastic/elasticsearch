/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.zen;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.discovery.SeedHostsProvider;
import org.elasticsearch.discovery.SeedHostsResolver;
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.Transport.Connection;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

public class UnicastZenPing implements ZenPing {

    private static final Logger logger = LogManager.getLogger(UnicastZenPing.class);

    public static final String ACTION_NAME = "internal:discovery/zen/unicast";

    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final ClusterName clusterName;

    private final PingContextProvider contextProvider;

    private final AtomicInteger pingingRoundIdGenerator = new AtomicInteger();

    private final Map<Integer, PingingRound> activePingingRounds = newConcurrentMap();

    // a list of temporal responses a node will return for a request (holds responses from other nodes)
    private final Queue<PingResponse> temporalResponses = ConcurrentCollections.newQueue();

    private final SeedHostsProvider hostsProvider;

    protected final EsThreadPoolExecutor unicastZenPingExecutorService;

    private final TimeValue resolveTimeout;

    private final String nodeName;

    private volatile boolean closed = false;

    public UnicastZenPing(
        Settings settings,
        ThreadPool threadPool,
        TransportService transportService,
        SeedHostsProvider seedHostsProvider,
        PingContextProvider contextProvider
    ) {
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        this.hostsProvider = seedHostsProvider;
        this.contextProvider = contextProvider;

        final int concurrentConnects = SeedHostsResolver.getMaxConcurrentResolvers(settings);
        resolveTimeout = SeedHostsResolver.getResolveTimeout(settings);
        nodeName = Node.NODE_NAME_SETTING.get(settings);
        logger.debug("using max_concurrent_resolvers [{}], resolver timeout [{}]", concurrentConnects, resolveTimeout);

        transportService.registerRequestHandler(
            ACTION_NAME,
            ThreadPool.Names.SAME,
            UnicastPingRequest::new,
            new UnicastPingRequestHandler()
        );

        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(settings, "[unicast_connect]");
        unicastZenPingExecutorService = EsExecutors.newScaling(
            nodeName + "/" + "unicast_connect",
            0,
            concurrentConnects,
            60,
            TimeUnit.SECONDS,
            false,
            threadFactory,
            threadPool.getThreadContext()
        );
    }

    private SeedHostsProvider.HostsResolver createHostsResolver() {
        return hosts -> SeedHostsResolver.resolveHostsLists(new CancellableThreads() {
            public void execute(Interruptible interruptible) {
                try {
                    interruptible.run();
                } catch (InterruptedException e) {
                    throw new CancellableThreads.ExecutionCancelledException("interrupted by " + e);
                }
            }
        }, unicastZenPingExecutorService, logger, hosts, transportService, resolveTimeout);
    }

    @Override
    public void close() {
        ThreadPool.terminate(unicastZenPingExecutorService, 10, TimeUnit.SECONDS);
        Releasables.close(activePingingRounds.values());
        closed = true;
    }

    @Override
    public void start() {}

    /**
     * Clears the list of cached ping responses.
     */
    public void clearTemporalResponses() {
        temporalResponses.clear();
    }

    /**
     * Sends three rounds of pings notifying the specified {@link Consumer} when pinging is complete. Pings are sent after resolving
     * configured unicast hosts to their IP address (subject to DNS caching within the JVM). A batch of pings is sent, then another batch
     * of pings is sent at half the specified {@link TimeValue}, and then another batch of pings is sent at the specified {@link TimeValue}.
     * The pings that are sent carry a timeout of 1.25 times the specified {@link TimeValue}. When pinging each node, a connection and
     * handshake is performed, with a connection timeout of the specified {@link TimeValue}.
     *
     * @param resultsConsumer the callback when pinging is complete
     * @param duration        the timeout for various components of the pings
     */
    @Override
    public void ping(final Consumer<PingCollection> resultsConsumer, final TimeValue duration) {
        ping(resultsConsumer, duration, duration);
    }

    /**
     * a variant of {@link #ping(Consumer, TimeValue)}, but allows separating the scheduling duration
     * from the duration used for request level time outs. This is useful for testing
     */
    protected void ping(final Consumer<PingCollection> resultsConsumer, final TimeValue scheduleDuration, final TimeValue requestDuration) {
        final List<TransportAddress> seedAddresses = new ArrayList<>();
        seedAddresses.addAll(hostsProvider.getSeedAddresses(createHostsResolver()));
        final DiscoveryNodes nodes = contextProvider.clusterState().nodes();
        // add all possible master nodes that were active in the last known cluster configuration
        for (DiscoveryNode masterNode : nodes.getMasterNodes().values()) {
            seedAddresses.add(masterNode.getAddress());
        }

        final ConnectionProfile connectionProfile = ConnectionProfile.buildSingleChannelProfile(
            TransportRequestOptions.Type.REG,
            requestDuration,
            requestDuration,
            TimeValue.MINUS_ONE,
            null,
            null
        );
        final PingingRound pingingRound = new PingingRound(
            pingingRoundIdGenerator.incrementAndGet(),
            seedAddresses,
            resultsConsumer,
            nodes.getLocalNode(),
            connectionProfile
        );
        activePingingRounds.put(pingingRound.id(), pingingRound);
        final AbstractRunnable pingSender = new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                if (e instanceof AlreadyClosedException == false) {
                    logger.warn("unexpected error while pinging", e);
                }
            }

            @Override
            protected void doRun() throws Exception {
                sendPings(requestDuration, pingingRound);
            }
        };
        threadPool.generic().execute(pingSender);
        threadPool.schedule(pingSender, TimeValue.timeValueMillis(scheduleDuration.millis() / 3), ThreadPool.Names.GENERIC);
        threadPool.schedule(pingSender, TimeValue.timeValueMillis(scheduleDuration.millis() / 3 * 2), ThreadPool.Names.GENERIC);
        threadPool.schedule(new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                finishPingingRound(pingingRound);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("unexpected error while finishing pinging round", e);
            }
        }, scheduleDuration, ThreadPool.Names.GENERIC);
    }

    // for testing
    protected void finishPingingRound(PingingRound pingingRound) {
        pingingRound.close();
    }

    protected class PingingRound implements Releasable {
        private final int id;
        private final Map<TransportAddress, Connection> tempConnections = new HashMap<>();
        private final KeyedLock<TransportAddress> connectionLock = new KeyedLock<>(true);
        private final PingCollection pingCollection;
        private final List<TransportAddress> seedAddresses;
        private final Consumer<PingCollection> pingListener;
        private final DiscoveryNode localNode;
        private final ConnectionProfile connectionProfile;

        private AtomicBoolean closed = new AtomicBoolean(false);

        PingingRound(
            int id,
            List<TransportAddress> seedAddresses,
            Consumer<PingCollection> resultsConsumer,
            DiscoveryNode localNode,
            ConnectionProfile connectionProfile
        ) {
            this.id = id;
            this.seedAddresses = Collections.unmodifiableList(seedAddresses.stream().distinct().collect(Collectors.toList()));
            this.pingListener = resultsConsumer;
            this.localNode = localNode;
            this.connectionProfile = connectionProfile;
            this.pingCollection = new PingCollection();
        }

        public int id() {
            return this.id;
        }

        public boolean isClosed() {
            return this.closed.get();
        }

        public List<TransportAddress> getSeedAddresses() {
            ensureOpen();
            return seedAddresses;
        }

        public Connection getOrConnect(DiscoveryNode node) throws IOException {
            Connection result;
            try (Releasable ignore = connectionLock.acquire(node.getAddress())) {
                result = tempConnections.get(node.getAddress());
                if (result == null) {
                    ensureOpen();
                    boolean success = false;
                    logger.trace("[{}] opening connection to [{}]", id(), node);
                    result = transportService.openConnection(node, connectionProfile);
                    try {
                        Connection finalResult = result;
                        PlainActionFuture.get(
                            fut -> transportService.handshake(finalResult, connectionProfile.getHandshakeTimeout(), fut.map(x -> null))
                        );
                        synchronized (this) {
                            // acquire lock and check if closed, to prevent leaving an open connection after closing
                            ensureOpen();
                            Connection existing = tempConnections.put(node.getAddress(), result);
                            assert existing == null;
                            success = true;
                        }
                    } finally {
                        if (success == false) {
                            logger.trace("[{}] closing connection to [{}] due to failure", id(), node);
                            IOUtils.closeWhileHandlingException(result);
                        }
                    }
                }
            }
            return result;
        }

        private void ensureOpen() {
            if (isClosed()) {
                throw new AlreadyClosedException("pinging round [" + id + "] is finished");
            }
        }

        public void addPingResponseToCollection(PingResponse pingResponse) {
            if (localNode.equals(pingResponse.node()) == false) {
                pingCollection.addPing(pingResponse);
            }
        }

        @Override
        public void close() {
            List<Connection> toClose = null;
            synchronized (this) {
                if (closed.compareAndSet(false, true)) {
                    activePingingRounds.remove(id);
                    toClose = new ArrayList<>(tempConnections.values());
                    tempConnections.clear();
                }
            }
            if (toClose != null) {
                // we actually closed
                try {
                    pingListener.accept(pingCollection);
                } finally {
                    IOUtils.closeWhileHandlingException(toClose);
                }
            }
        }

        public ConnectionProfile getConnectionProfile() {
            return connectionProfile;
        }
    }

    protected void sendPings(final TimeValue timeout, final PingingRound pingingRound) {
        final ClusterState lastState = contextProvider.clusterState();
        final UnicastPingRequest pingRequest = new UnicastPingRequest(pingingRound.id(), timeout, createPingResponse(lastState));

        List<TransportAddress> temporalAddresses = temporalResponses.stream().map(pingResponse -> {
            assert clusterName.equals(pingResponse.clusterName())
                : "got a ping request from a different cluster. expected " + clusterName + " got " + pingResponse.clusterName();
            return pingResponse.node().getAddress();
        }).collect(Collectors.toList());

        final Stream<TransportAddress> uniqueAddresses = Stream.concat(pingingRound.getSeedAddresses().stream(), temporalAddresses.stream())
            .distinct();

        // resolve what we can via the latest cluster state
        final Set<DiscoveryNode> nodesToPing = uniqueAddresses.map(address -> {
            DiscoveryNode foundNode = lastState.nodes().findByAddress(address);
            if (foundNode != null && transportService.nodeConnected(foundNode)) {
                return foundNode;
            } else {
                return new DiscoveryNode(
                    address.toString(),
                    address,
                    emptyMap(),
                    emptySet(),
                    Version.CURRENT.minimumCompatibilityVersion()
                );
            }
        }).collect(Collectors.toSet());

        nodesToPing.forEach(node -> sendPingRequestToNode(node, timeout, pingingRound, pingRequest));
    }

    private void sendPingRequestToNode(
        final DiscoveryNode node,
        TimeValue timeout,
        final PingingRound pingingRound,
        final UnicastPingRequest pingRequest
    ) {
        submitToExecutor(new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                Connection connection = null;
                if (transportService.nodeConnected(node)) {
                    try {
                        // concurrency can still cause disconnects
                        connection = transportService.getConnection(node);
                    } catch (NodeNotConnectedException e) {
                        logger.trace("[{}] node [{}] just disconnected, will create a temp connection", pingingRound.id(), node);
                    }
                }

                if (connection == null) {
                    connection = pingingRound.getOrConnect(node);
                }

                logger.trace("[{}] sending to {}", pingingRound.id(), node);
                transportService.sendRequest(
                    connection,
                    ACTION_NAME,
                    pingRequest,
                    TransportRequestOptions.timeout(TimeValue.timeValueMillis((long) (timeout.millis() * 1.25))),
                    getPingResponseHandler(pingingRound, node)
                );
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof ConnectTransportException || e instanceof AlreadyClosedException) {
                    // can't connect to the node - this is more common path!
                    logger.trace(() -> new ParameterizedMessage("[{}] failed to ping {}", pingingRound.id(), node), e);
                } else if (e instanceof RemoteTransportException) {
                    // something went wrong on the other side
                    logger.debug(
                        () -> new ParameterizedMessage("[{}] received a remote error as a response to ping {}", pingingRound.id(), node),
                        e
                    );
                } else {
                    logger.warn(() -> new ParameterizedMessage("[{}] failed send ping to {}", pingingRound.id(), node), e);
                }
            }

            @Override
            public void onRejection(Exception e) {
                // The RejectedExecutionException can come from the fact unicastZenPingExecutorService is at its max down in sendPings
                // But don't bail here, we can retry later on after the send ping has been scheduled.
                logger.debug("Ping execution rejected", e);
            }
        });
    }

    // for testing
    protected void submitToExecutor(AbstractRunnable abstractRunnable) {
        unicastZenPingExecutorService.execute(abstractRunnable);
    }

    // for testing
    protected TransportResponseHandler<UnicastPingResponse> getPingResponseHandler(
        final PingingRound pingingRound,
        final DiscoveryNode node
    ) {
        return new TransportResponseHandler<UnicastPingResponse>() {

            @Override
            public UnicastPingResponse read(StreamInput in) throws IOException {
                return new UnicastPingResponse(in);
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public void handleResponse(UnicastPingResponse response) {
                logger.trace("[{}] received response from {}: {}", pingingRound.id(), node, Arrays.toString(response.pingResponses));
                if (pingingRound.isClosed()) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("[{}] skipping received response from {}. already closed", pingingRound.id(), node);
                    }
                } else {
                    Stream.of(response.pingResponses).forEach(pingingRound::addPingResponseToCollection);
                }
            }

            @Override
            public void handleException(TransportException exp) {
                if (exp instanceof ConnectTransportException
                    || exp.getCause() instanceof ConnectTransportException
                    || exp.getCause() instanceof AlreadyClosedException) {
                    // ok, not connected...
                    logger.trace(() -> new ParameterizedMessage("failed to connect to {}", node), exp);
                } else if (closed == false) {
                    logger.warn(() -> new ParameterizedMessage("failed to send ping to [{}]", node), exp);
                }
            }
        };
    }

    private UnicastPingResponse handlePingRequest(final UnicastPingRequest request) {
        assert clusterName.equals(request.pingResponse.clusterName())
            : "got a ping request from a different cluster. expected " + clusterName + " got " + request.pingResponse.clusterName();
        temporalResponses.add(request.pingResponse);
        // add to any ongoing pinging
        activePingingRounds.values().forEach(p -> p.addPingResponseToCollection(request.pingResponse));
        threadPool.schedule(
            () -> temporalResponses.remove(request.pingResponse),
            TimeValue.timeValueMillis(request.timeout.millis() * 2),
            ThreadPool.Names.SAME
        );

        List<PingResponse> pingResponses = CollectionUtils.iterableAsArrayList(temporalResponses);
        pingResponses.add(createPingResponse(contextProvider.clusterState()));

        return new UnicastPingResponse(request.id, pingResponses.toArray(new PingResponse[pingResponses.size()]));
    }

    class UnicastPingRequestHandler implements TransportRequestHandler<UnicastPingRequest> {

        @Override
        public void messageReceived(UnicastPingRequest request, TransportChannel channel, Task task) throws Exception {
            if (closed) {
                throw new AlreadyClosedException("node is shutting down");
            }
            if (request.pingResponse.clusterName().equals(clusterName)) {
                channel.sendResponse(handlePingRequest(request));
            } else {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "mismatched cluster names; request: [%s], local: [%s]",
                        request.pingResponse.clusterName().value(),
                        clusterName.value()
                    )
                );
            }
        }

    }

    public static class UnicastPingRequest extends TransportRequest {

        public final int id;
        public final TimeValue timeout;
        public final PingResponse pingResponse;

        public UnicastPingRequest(int id, TimeValue timeout, PingResponse pingResponse) {
            this.id = id;
            this.timeout = timeout;
            this.pingResponse = pingResponse;
        }

        public UnicastPingRequest(StreamInput in) throws IOException {
            super(in);
            id = in.readInt();
            timeout = in.readTimeValue();
            pingResponse = new PingResponse(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(id);
            out.writeTimeValue(timeout);
            pingResponse.writeTo(out);
        }
    }

    private PingResponse createPingResponse(ClusterState clusterState) {
        DiscoveryNodes discoNodes = clusterState.nodes();
        return new PingResponse(discoNodes.getLocalNode(), discoNodes.getMasterNode(), clusterState);
    }

    public static class UnicastPingResponse extends TransportResponse {

        final int id;

        public final PingResponse[] pingResponses;

        public UnicastPingResponse(int id, PingResponse[] pingResponses) {
            this.id = id;
            this.pingResponses = pingResponses;
        }

        public UnicastPingResponse(StreamInput in) throws IOException {
            id = in.readInt();
            pingResponses = new PingResponse[in.readVInt()];
            for (int i = 0; i < pingResponses.length; i++) {
                pingResponses[i] = new PingResponse(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(id);
            out.writeVInt(pingResponses.length);
            for (PingResponse pingResponse : pingResponses) {
                pingResponse.writeTo(out);
            }
        }
    }

    protected Version getVersion() {
        return Version.CURRENT; // for tests
    }

}
