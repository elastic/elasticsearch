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

package org.elasticsearch.discovery.zen;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.discovery.zen.ZenPing.PingResponse.readPingResponse;

public class UnicastZenPing extends AbstractComponent implements ZenPing {

    public static final String ACTION_NAME = "internal:discovery/zen/unicast";
    public static final Setting<List<String>> DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING =
        Setting.listSetting("discovery.zen.ping.unicast.hosts", emptyList(), Function.identity(),
            Property.NodeScope);
    public static final Setting<Integer> DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING =
        Setting.intSetting("discovery.zen.ping.unicast.concurrent_connects", 10, 0, Property.NodeScope);
    public static final Setting<TimeValue> DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT =
        Setting.positiveTimeSetting("discovery.zen.ping.unicast.hosts.resolve_timeout", TimeValue.timeValueSeconds(5), Property.NodeScope);

    // these limits are per-address
    public static final int LIMIT_FOREIGN_PORTS_COUNT = 1;
    public static final int LIMIT_LOCAL_PORTS_COUNT = 5;

    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final ClusterName clusterName;

    private final int concurrentConnects;

    private final List<String> configuredHosts;

    private final int limitPortCounts;

    private volatile PingContextProvider contextProvider;

    private final AtomicInteger pingingRoundIdGenerator = new AtomicInteger();

    // used as a node id prefix for configured unicast host nodes/address
    private static final String UNICAST_NODE_PREFIX = "#zen_unicast_";

    private final Map<Integer, PingingRound> activePingingRounds = newConcurrentMap();

    // a list of temporal responses a node will return for a request (holds responses from other nodes)
    private final Queue<PingResponse> temporalResponses = ConcurrentCollections.newQueue();

    private final UnicastHostsProvider hostsProvider;

    protected final EsThreadPoolExecutor unicastZenPingExecutorService;

    private final TimeValue resolveTimeout;

    private volatile boolean closed = false;

    public UnicastZenPing(Settings settings, ThreadPool threadPool, TransportService transportService,
                          UnicastHostsProvider unicastHostsProvider) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        this.hostsProvider = unicastHostsProvider;

        this.concurrentConnects = DISCOVERY_ZEN_PING_UNICAST_CONCURRENT_CONNECTS_SETTING.get(settings);
        final List<String> hosts = DISCOVERY_ZEN_PING_UNICAST_HOSTS_SETTING.get(settings);
        if (hosts.isEmpty()) {
            // if unicast hosts are not specified, fill with simple defaults on the local machine
            configuredHosts = transportService.getLocalAddresses();
            limitPortCounts = LIMIT_LOCAL_PORTS_COUNT;
        } else {
            configuredHosts = hosts;
            // we only limit to 1 addresses, makes no sense to ping 100 ports
            limitPortCounts = LIMIT_FOREIGN_PORTS_COUNT;
        }
        resolveTimeout = DISCOVERY_ZEN_PING_UNICAST_HOSTS_RESOLVE_TIMEOUT.get(settings);
        logger.debug(
            "using initial hosts {}, with concurrent_connects [{}], resolve_timeout [{}]",
            configuredHosts,
            concurrentConnects,
            resolveTimeout);

        transportService.registerRequestHandler(ACTION_NAME, UnicastPingRequest::new, ThreadPool.Names.SAME,
            new UnicastPingRequestHandler());

        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(settings, "[unicast_connect]");
        unicastZenPingExecutorService = EsExecutors.newScaling(
            "unicast_connect",
            0, concurrentConnects,
            60,
            TimeUnit.SECONDS,
            threadFactory,
            threadPool.getThreadContext());
    }

    /**
     * Resolves a list of hosts to a list of discovery nodes. Each host is resolved into a transport address (or a collection of addresses
     * if the number of ports is greater than one) and the transport addresses are used to created discovery nodes. Host lookups are done
     * in parallel using specified executor service up to the specified resolve timeout.
     *
     * @param executorService  the executor service used to parallelize hostname lookups
     * @param logger           logger used for logging messages regarding hostname lookups
     * @param hosts            the hosts to resolve
     * @param limitPortCounts  the number of ports to resolve (should be 1 for non-local transport)
     * @param transportService the transport service
     * @param nodeId_prefix    a prefix to use for node ids
     * @param resolveTimeout   the timeout before returning from hostname lookups
     * @return a list of discovery nodes with resolved transport addresses
     */
    public static List<DiscoveryNode> resolveHostsLists(
        final ExecutorService executorService,
        final Logger logger,
        final List<String> hosts,
        final int limitPortCounts,
        final TransportService transportService,
        final String nodeId_prefix,
        final TimeValue resolveTimeout) throws InterruptedException {
        Objects.requireNonNull(executorService);
        Objects.requireNonNull(logger);
        Objects.requireNonNull(hosts);
        Objects.requireNonNull(transportService);
        Objects.requireNonNull(nodeId_prefix);
        Objects.requireNonNull(resolveTimeout);
        if (resolveTimeout.nanos() < 0) {
            throw new IllegalArgumentException("resolve timeout must be non-negative but was [" + resolveTimeout + "]");
        }
        // create tasks to submit to the executor service; we will wait up to resolveTimeout for these tasks to complete
        final List<Callable<TransportAddress[]>> callables =
            hosts
                .stream()
                .map(hn -> (Callable<TransportAddress[]>) () -> transportService.addressesFromString(hn, limitPortCounts))
                .collect(Collectors.toList());
        final List<Future<TransportAddress[]>> futures =
            executorService.invokeAll(callables, resolveTimeout.nanos(), TimeUnit.NANOSECONDS);
        final List<DiscoveryNode> discoveryNodes = new ArrayList<>();
        // ExecutorService#invokeAll guarantees that the futures are returned in the iteration order of the tasks so we can associate the
        // hostname with the corresponding task by iterating together
        final Iterator<String> it = hosts.iterator();
        for (final Future<TransportAddress[]> future : futures) {
            final String hostname = it.next();
            if (!future.isCancelled()) {
                assert future.isDone();
                try {
                    final TransportAddress[] addresses = future.get();
                    logger.trace("resolved host [{}] to {}", hostname, addresses);
                    for (int addressId = 0; addressId < addresses.length; addressId++) {
                        discoveryNodes.add(
                            new DiscoveryNode(
                                nodeId_prefix + "_" + hostname + "_" + addressId + "#",
                                addresses[addressId],
                                emptyMap(),
                                emptySet(),
                                Version.CURRENT.minimumCompatibilityVersion()));
                    }
                } catch (final ExecutionException e) {
                    assert e.getCause() != null;
                    final String message = "failed to resolve host [" + hostname + "]";
                    logger.warn(message, e.getCause());
                }
            } else {
                logger.warn("timed out after [{}] resolving host [{}]", resolveTimeout, hostname);
            }
        }
        return discoveryNodes;
    }

    @Override
    public void close() {
        ThreadPool.terminate(unicastZenPingExecutorService, 0, TimeUnit.SECONDS);
        Releasables.close(activePingingRounds.values());
        closed = true;
    }

    @Override
    public void start(PingContextProvider contextProvider) {
        this.contextProvider = contextProvider;
    }

    /**
     * Clears the list of cached ping responses.
     */
    public void clearTemporalResponses() {
        temporalResponses.clear();
    }

    // test only
    ZenPing.PingCollection pingAndWait(TimeValue timeout) throws ExecutionException, InterruptedException {
        final CompletableFuture<PingCollection> response = new CompletableFuture<>();
        try {
            ping(response::complete, timeout);
        } catch (Exception ex) {
            response.completeExceptionally(ex);
        }
        return response.get();
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
        final List<DiscoveryNode> seedNodes;
        try {
            seedNodes = resolveHostsLists(
                unicastZenPingExecutorService,
                logger,
                configuredHosts,
                limitPortCounts,
                transportService,
                UNICAST_NODE_PREFIX,
                resolveTimeout);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        seedNodes.addAll(hostsProvider.buildDynamicNodes());
        final DiscoveryNodes nodes = contextProvider.nodes();
        // add all possible master nodes that were active in the last known cluster configuration
        for (ObjectCursor<DiscoveryNode> masterNode : nodes.getMasterNodes().values()) {
            seedNodes.add(masterNode.value);
        }

        final PingingRound pingingRound = new PingingRound(pingingRoundIdGenerator.incrementAndGet(), seedNodes, resultsConsumer,
            nodes.getLocalNode());
        activePingingRounds.put(pingingRound.id(), pingingRound);
        final AbstractRunnable pingSender = new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                if (e instanceof AlreadyClosedException) {
                } else {
                    logger.warn("unexpected error while pinging", e);
                }
            }

            @Override
            protected void doRun() throws Exception {
                sendPings(duration, pingingRound);
            }
        };
        threadPool.generic().execute(pingSender);
        threadPool.schedule(TimeValue.timeValueMillis(duration.millis() / 3), ThreadPool.Names.GENERIC, pingSender);
        threadPool.schedule(TimeValue.timeValueMillis(duration.millis() / 3 * 2), ThreadPool.Names.GENERIC, pingSender);
        threadPool.schedule(duration, ThreadPool.Names.GENERIC, new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                finishPingingRound(pingingRound);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("unexpected error while finishing pinging round", e);
            }
        });
    }

    // for testing
    protected void finishPingingRound(PingingRound pingingRound) {
        pingingRound.close();
    }

    class PingingRound implements Releasable {
        private final int id;
        private final Map<TransportAddress, Connection> tempConnections = new HashMap<>();
        private final PingCollection pingCollection;
        private final List<DiscoveryNode> seedNodes;
        private final Consumer<PingCollection> pingListener;
        private final DiscoveryNode localNode;

        private AtomicBoolean closed = new AtomicBoolean(false);

        PingingRound(int id, List<DiscoveryNode> seedNodes, Consumer<PingCollection> resultsConsumer, DiscoveryNode localNode) {
            this.id = id;
            this.seedNodes = Collections.unmodifiableList(new ArrayList<>(seedNodes));
            this.pingListener = resultsConsumer;
            this.localNode = localNode;
            this.pingCollection = new PingCollection();
        }

        public int id() {
            return this.id;
        }

        public boolean isClosed() {
            return this.closed.get();
        }

        public List<DiscoveryNode> getSeedNodes() {
            checkIfClosed();
            return seedNodes;
        }

        public synchronized Connection getConnection(TransportAddress address) {
            checkIfClosed();
            return tempConnections.get(address);
        }

        private void checkIfClosed() {
            if (isClosed()) {
                throw new AlreadyClosedException("pinging round [" + id + "] is finished");
            }
        }

        public synchronized Connection addConnectionIfNeeded(TransportAddress address, Connection newConnection) {
            final Connection existing = getConnection(address);
            if (existing == null) {
                tempConnections.put(address, newConnection);
                return newConnection;
            } else {
                IOUtils.closeWhileHandlingException(newConnection);
                return existing;
            }
        }

        public void addPingResponseToCollection(PingResponse pingResponse) {
            if (pingResponse.node().equals(localNode) == false) {
                pingCollection.addPing(pingResponse);
            }
        }


        public PingCollection pingCollection() {
            return pingCollection;
        }

        @Override
        public void close() {
            List<Connection> toClose = null;
            synchronized (this) {
                if (closed.compareAndSet(false, true)) {
                    activePingingRounds.remove(id);
                    toClose = new ArrayList<>(tempConnections.values());
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
    }


    void sendPings(final TimeValue timeout, final PingingRound pingingRound) {
        final UnicastPingRequest pingRequest = new UnicastPingRequest();
        pingRequest.id = pingingRound.id();
        pingRequest.timeout = timeout;
        DiscoveryNodes discoNodes = contextProvider.nodes();

        pingRequest.pingResponse = createPingResponse(discoNodes);

        HashSet<DiscoveryNode> nodesToPingSet = new HashSet<>();
        for (PingResponse temporalResponse : temporalResponses) {
            // Only send pings to nodes that have the same cluster name.
            if (clusterName.equals(temporalResponse.clusterName())) {
                nodesToPingSet.add(temporalResponse.node());
            }
        }

        // sort the nodes by likelihood of being an active master
        List<DiscoveryNode> sortedNodesToPing = ElectMasterService.sortByMasterLikelihood(nodesToPingSet);
        // add the configured hosts first
        final List<DiscoveryNode> nodesToPing =
            Stream.concat(pingingRound.getSeedNodes().stream(), sortedNodesToPing.stream())
                .map(node -> {
                    DiscoveryNode foundNode = discoNodes.findByAddress(node.getAddress());
                    if (foundNode == null) {
                        return node;
                    } else {
                        return foundNode;
                    }
                }).collect(Collectors.toList());

        for (final DiscoveryNode node : nodesToPing) {
            unicastZenPingExecutorService.execute(new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    Connection connection = null;
                    if (transportService.nodeConnected(node)) {
                        try {
                            // concurrency can still cause disconnects
                            connection = transportService.getConnection(node);
                        } catch (NodeNotConnectedException e) {
                            // meh
                        }
                    } else {
                        connection = pingingRound.getConnection(node.getAddress());
                    }

                    if (connection == null) {
                        logger.trace("[{}] connecting (light) to {}", pingingRound.id(), node);
                        boolean success = false;
                        connection = transportService.openConnection(node, ConnectionProfile.LIGHT_PROFILE);
                        try {
                            transportService.handshake(connection, timeout.millis());
                            connection = pingingRound.addConnectionIfNeeded(node.getAddress(), connection);
                            success = true;
                        } finally {
                            if (success == false) {
                                IOUtils.closeWhileHandlingException(connection);
                            }
                        }
                    }

                    sendPingRequestToNode(pingingRound, timeout, pingRequest, connection, node);
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof ConnectTransportException) {
                        // can't connect to the node - this is more common path!
                        logger.trace(
                            (Supplier<?>) () -> new ParameterizedMessage(
                                "[{}] failed to connect to {}", pingingRound.id(), node), e);
                    } else if (e instanceof RemoteTransportException) {
                        // something went wrong on the other side
                        logger.debug(
                            (Supplier<?>) () -> new ParameterizedMessage(
                                "[{}] received a remote error as a response to ping {}", pingingRound.id(), node), e);
                    } else {
                        logger.warn(
                            (Supplier<?>) () -> new ParameterizedMessage(
                                "[{}] failed send ping to {}", pingingRound.id(), node), e);
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
    }

    private void sendPingRequestToNode(final PingingRound pingingRound, final TimeValue timeout, final UnicastPingRequest pingRequest,
                                       Connection connection, final DiscoveryNode node) {
        logger.trace("[{}] sending to {}", pingingRound.id(), node);
        transportService.sendRequest(connection, ACTION_NAME, pingRequest,
            TransportRequestOptions.builder().withTimeout((long) (timeout.millis() * 1.25)).build(),
            getPingResponseHandler(pingingRound, node));
    }

    // for testing
    protected TransportResponseHandler<UnicastPingResponse> getPingResponseHandler(final PingingRound pingingRound, final DiscoveryNode node) {
        return new TransportResponseHandler<UnicastPingResponse>() {

            @Override
            public UnicastPingResponse newInstance() {
                return new UnicastPingResponse();
            }

            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }

            @Override
            public void handleResponse(UnicastPingResponse response) {
                logger.trace("[{}] received response from {}: {}", pingingRound, node, Arrays.toString(response.pingResponses));
                if (pingingRound.isClosed() == false) {
                    Arrays.asList(response.pingResponses).forEach(pingingRound::addPingResponseToCollection);
                }
            }

            @Override
            public void handleException(TransportException exp) {
                if (exp instanceof ConnectTransportException) {
                    // ok, not connected...
                    logger.trace((Supplier<?>) () -> new ParameterizedMessage("failed to connect to {}", node), exp);
                } else {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to send ping to [{}]", node), exp);
                }
            }
        };
    }

    private UnicastPingResponse handlePingRequest(final UnicastPingRequest request) {
        temporalResponses.add(request.pingResponse);
        // add to any ongoing pinging
        activePingingRounds.values().forEach(p -> p.addPingResponseToCollection(request.pingResponse));
        threadPool.schedule(TimeValue.timeValueMillis(request.timeout.millis() * 2), ThreadPool.Names.SAME,
            () -> temporalResponses.remove(request.pingResponse));

        List<PingResponse> pingResponses = CollectionUtils.iterableAsArrayList(temporalResponses);
        pingResponses.add(createPingResponse(contextProvider.nodes()));

        UnicastPingResponse unicastPingResponse = new UnicastPingResponse();
        unicastPingResponse.id = request.id;
        unicastPingResponse.pingResponses = pingResponses.toArray(new PingResponse[pingResponses.size()]);

        return unicastPingResponse;
    }

    class UnicastPingRequestHandler implements TransportRequestHandler<UnicastPingRequest> {

        @Override
        public void messageReceived(UnicastPingRequest request, TransportChannel channel) throws Exception {
            if (request.pingResponse.clusterName().equals(clusterName)) {
                channel.sendResponse(handlePingRequest(request));
            } else {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "mismatched cluster names; request: [%s], local: [%s]",
                        request.pingResponse.clusterName().value(),
                        clusterName.value()));
            }
        }

    }

    public static class UnicastPingRequest extends TransportRequest {

        int id;
        TimeValue timeout;
        PingResponse pingResponse;

        public UnicastPingRequest() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            id = in.readInt();
            timeout = new TimeValue(in);
            pingResponse = readPingResponse(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeInt(id);
            timeout.writeTo(out);
            pingResponse.writeTo(out);
        }
    }

    private PingResponse createPingResponse(DiscoveryNodes discoNodes) {
        return new PingResponse(discoNodes.getLocalNode(), discoNodes.getMasterNode(), contextProvider.clusterState());
    }

    static class UnicastPingResponse extends TransportResponse {

        int id;

        PingResponse[] pingResponses;

        UnicastPingResponse() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            id = in.readInt();
            pingResponses = new PingResponse[in.readVInt()];
            for (int i = 0; i < pingResponses.length; i++) {
                pingResponses[i] = readPingResponse(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
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
