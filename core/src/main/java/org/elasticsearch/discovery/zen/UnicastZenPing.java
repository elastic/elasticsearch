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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
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
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.RemoteTransportException;
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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    private final AtomicInteger pingHandlerIdGenerator = new AtomicInteger();

    // used to generate unique ids for nodes/address we temporarily connect to
    private final AtomicInteger unicastNodeIdGenerator = new AtomicInteger();

    // used as a node id prefix for nodes/address we temporarily connect to
    private static final String UNICAST_NODE_PREFIX = "#zen_unicast_";

    private final Map<Integer, SendPingsHandler> receivedResponses = newConcurrentMap();

    // a list of temporal responses a node will return for a request (holds responses from other nodes)
    private final Queue<PingResponse> temporalResponses = ConcurrentCollections.newQueue();

    private final UnicastHostsProvider hostsProvider;

    private final ExecutorService unicastZenPingExecutorService;

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
     * @param idGenerator      the generator to supply unique ids for each discovery node
     * @param resolveTimeout   the timeout before returning from hostname lookups
     * @return a list of discovery nodes with resolved transport addresses
     */
    public static List<DiscoveryNode> resolveDiscoveryNodes(
        final ExecutorService executorService,
        final Logger logger,
        final List<String> hosts,
        final int limitPortCounts,
        final TransportService transportService,
        final Supplier<String> idGenerator,
        final TimeValue resolveTimeout) throws InterruptedException {
        Objects.requireNonNull(executorService);
        Objects.requireNonNull(logger);
        Objects.requireNonNull(hosts);
        Objects.requireNonNull(transportService);
        Objects.requireNonNull(idGenerator);
        Objects.requireNonNull(resolveTimeout);
        if (resolveTimeout.nanos() < 0) {
            throw new IllegalArgumentException("resolve timeout must be non-negative but was [" + resolveTimeout + "]");
        }
        // create tasks to submit to the executor service; we will wait up to resolveTimeout for these tasks to complete
        final List<Callable<TransportAddress[]>> callables =
            hosts
                .stream()
                .map(hn -> (Callable<TransportAddress[]>)() -> transportService.addressesFromString(hn, limitPortCounts))
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
                    for (final TransportAddress address : addresses) {
                        discoveryNodes.add(
                            new DiscoveryNode(
                                idGenerator.get(),
                                address,
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
        Releasables.close(receivedResponses.values());
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
    Collection<PingResponse> pingAndWait(TimeValue duration) {
        final AtomicReference<Collection<PingResponse>> response = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ping(pings -> {
            response.set(pings);
            latch.countDown();
        }, duration);
        try {
            latch.await();
            return response.get();
        } catch (InterruptedException e) {
            return null;
        }
    }

    /**
     * Sends three rounds of pings notifying the specified {@link PingListener} when pinging is complete. Pings are sent after resolving
     * configured unicast hosts to their IP address (subject to DNS caching within the JVM). A batch of pings is sent, then another batch
     * of pings is sent at half the specified {@link TimeValue}, and then another batch of pings is sent at the specified {@link TimeValue}.
     * The pings that are sent carry a timeout of 1.25 times the specified {@link TimeValue}. When pinging each node, a connection and
     * handshake is performed, with a connection timeout of the specified {@link TimeValue}.
     *
     * @param listener the callback when pinging is complete
     * @param duration the timeout for various components of the pings
     */
    @Override
    public void ping(final PingListener listener, final TimeValue duration) {
        final List<DiscoveryNode> resolvedDiscoveryNodes;
        try {
            resolvedDiscoveryNodes = resolveDiscoveryNodes(
                unicastZenPingExecutorService,
                logger,
                configuredHosts,
                limitPortCounts,
                transportService,
                () -> UNICAST_NODE_PREFIX + unicastNodeIdGenerator.incrementAndGet() + "#",
                resolveTimeout);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        final SendPingsHandler sendPingsHandler = new SendPingsHandler(pingHandlerIdGenerator.incrementAndGet());
        try {
            receivedResponses.put(sendPingsHandler.id(), sendPingsHandler);
            try {
                sendPings(duration, null, sendPingsHandler, resolvedDiscoveryNodes);
            } catch (RejectedExecutionException e) {
                logger.debug("Ping execution rejected", e);
                // The RejectedExecutionException can come from the fact unicastZenPingExecutorService is at its max down in sendPings
                // But don't bail here, we can retry later on after the send ping has been scheduled.
            }

            threadPool.schedule(TimeValue.timeValueMillis(duration.millis() / 2), ThreadPool.Names.GENERIC, new AbstractRunnable() {
                @Override
                protected void doRun() {
                    sendPings(duration, null, sendPingsHandler, resolvedDiscoveryNodes);
                    threadPool.schedule(TimeValue.timeValueMillis(duration.millis() / 2), ThreadPool.Names.GENERIC, new AbstractRunnable() {
                        @Override
                        protected void doRun() throws Exception {
                            sendPings(duration, TimeValue.timeValueMillis(duration.millis() / 2), sendPingsHandler, resolvedDiscoveryNodes);
                            sendPingsHandler.close();
                            listener.onPing(sendPingsHandler.pingCollection().toList());
                            for (DiscoveryNode node : sendPingsHandler.nodeToDisconnect) {
                                logger.trace("[{}] disconnecting from {}", sendPingsHandler.id(), node);
                                transportService.disconnectFromNode(node);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.debug("Ping execution failed", e);
                            sendPingsHandler.close();
                        }
                    });
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug("Ping execution failed", e);
                    sendPingsHandler.close();
                }
            });
        } catch (EsRejectedExecutionException ex) { // TODO: remove this once ScheduledExecutor has support for AbstractRunnable
            sendPingsHandler.close();
            // we are shutting down
        } catch (Exception e) {
            sendPingsHandler.close();
            throw new ElasticsearchException("Ping execution failed", e);
        }
    }

    class SendPingsHandler implements Releasable {
        private final int id;
        private final Set<DiscoveryNode> nodeToDisconnect = ConcurrentCollections.newConcurrentSet();
        private final PingCollection pingCollection;

        private AtomicBoolean closed = new AtomicBoolean(false);

        SendPingsHandler(int id) {
            this.id = id;
            this.pingCollection = new PingCollection();
        }

        public int id() {
            return this.id;
        }

        public boolean isClosed() {
            return this.closed.get();
        }

        public PingCollection pingCollection() {
            return pingCollection;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                receivedResponses.remove(id);
            }
        }
    }


    void sendPings(
        final TimeValue timeout,
        @Nullable TimeValue waitTime,
        final SendPingsHandler sendPingsHandler,
        final List<DiscoveryNode> resolvedDiscoveryNodes) {
        final UnicastPingRequest pingRequest = new UnicastPingRequest();
        pingRequest.id = sendPingsHandler.id();
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
        nodesToPingSet.addAll(hostsProvider.buildDynamicNodes());

        // add all possible master nodes that were active in the last known cluster configuration
        for (ObjectCursor<DiscoveryNode> masterNode : discoNodes.getMasterNodes().values()) {
            nodesToPingSet.add(masterNode.value);
        }

        // sort the nodes by likelihood of being an active master
        List<DiscoveryNode> sortedNodesToPing = ElectMasterService.sortByMasterLikelihood(nodesToPingSet);

        // add the configured hosts first
        final List<DiscoveryNode> nodesToPing = new ArrayList<>(resolvedDiscoveryNodes.size() + sortedNodesToPing.size());
        nodesToPing.addAll(resolvedDiscoveryNodes);
        nodesToPing.addAll(sortedNodesToPing);

        final CountDownLatch latch = new CountDownLatch(nodesToPing.size());
        for (final DiscoveryNode node : nodesToPing) {
            // make sure we are connected
            final boolean nodeFoundByAddress;
            DiscoveryNode nodeToSend = discoNodes.findByAddress(node.getAddress());
            if (nodeToSend != null) {
                nodeFoundByAddress = true;
            } else {
                nodeToSend = node;
                nodeFoundByAddress = false;
            }

            if (!transportService.nodeConnected(nodeToSend)) {
                if (sendPingsHandler.isClosed()) {
                    return;
                }
                // if we find on the disco nodes a matching node by address, we are going to restore the connection
                // anyhow down the line if its not connected...
                // if we can't resolve the node, we don't know and we have to clean up after pinging. We do have
                // to make sure we don't disconnect a true node which was temporarily removed from the DiscoveryNodes
                // but will be added again during the pinging. We therefore create a new temporary node
                if (!nodeFoundByAddress) {
                    if (!nodeToSend.getId().startsWith(UNICAST_NODE_PREFIX)) {
                        DiscoveryNode tempNode = new DiscoveryNode("",
                            UNICAST_NODE_PREFIX + unicastNodeIdGenerator.incrementAndGet() + "_" + nodeToSend.getId() + "#",
                            UUIDs.randomBase64UUID(), nodeToSend.getHostName(), nodeToSend.getHostAddress(), nodeToSend.getAddress(),
                            nodeToSend.getAttributes(), nodeToSend.getRoles(), nodeToSend.getVersion());

                        logger.trace("replacing {} with temp node {}", nodeToSend, tempNode);
                        nodeToSend = tempNode;
                    }
                    sendPingsHandler.nodeToDisconnect.add(nodeToSend);
                }
                // fork the connection to another thread
                final DiscoveryNode finalNodeToSend = nodeToSend;
                unicastZenPingExecutorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        if (sendPingsHandler.isClosed()) {
                            return;
                        }
                        boolean success = false;
                        try {
                            // connect to the node, see if we manage to do it, if not, bail
                            if (!nodeFoundByAddress) {
                                logger.trace("[{}] connecting (light) to {}", sendPingsHandler.id(), finalNodeToSend);
                                transportService.connectToNodeLightAndHandshake(finalNodeToSend, timeout.getMillis());
                            } else {
                                logger.trace("[{}] connecting to {}", sendPingsHandler.id(), finalNodeToSend);
                                transportService.connectToNode(finalNodeToSend);
                            }
                            logger.trace("[{}] connected to {}", sendPingsHandler.id(), node);
                            if (receivedResponses.containsKey(sendPingsHandler.id())) {
                                // we are connected and still in progress, send the ping request
                                sendPingRequestToNode(sendPingsHandler.id(), timeout, pingRequest, latch, node, finalNodeToSend);
                            } else {
                                // connect took too long, just log it and bail
                                latch.countDown();
                                logger.trace("[{}] connect to {} was too long outside of ping window, bailing",
                                        sendPingsHandler.id(), node);
                            }
                            success = true;
                        } catch (ConnectTransportException e) {
                            // can't connect to the node - this is a more common path!
                            logger.trace(
                                (Supplier<?>) () -> new ParameterizedMessage(
                                    "[{}] failed to connect to {}", sendPingsHandler.id(), finalNodeToSend), e);
                        } catch (RemoteTransportException e) {
                            // something went wrong on the other side
                            logger.debug(
                                (Supplier<?>) () -> new ParameterizedMessage(
                                    "[{}] received a remote error as a response to ping {}", sendPingsHandler.id(), finalNodeToSend), e);
                        } catch (Exception e) {
                            logger.warn(
                                (Supplier<?>) () -> new ParameterizedMessage(
                                    "[{}] failed send ping to {}", sendPingsHandler.id(), finalNodeToSend), e);
                        } finally {
                            if (!success) {
                                latch.countDown();
                            }
                        }
                    }
                });
            } else {
                sendPingRequestToNode(sendPingsHandler.id(), timeout, pingRequest, latch, node, nodeToSend);
            }
        }
        if (waitTime != null) {
            try {
                latch.await(waitTime.millis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private void sendPingRequestToNode(final int id, final TimeValue timeout, final UnicastPingRequest pingRequest,
                                       final CountDownLatch latch, final DiscoveryNode node, final DiscoveryNode nodeToSend) {
        logger.trace("[{}] sending to {}", id, nodeToSend);
        transportService.sendRequest(nodeToSend, ACTION_NAME, pingRequest, TransportRequestOptions.builder()
                .withTimeout((long) (timeout.millis() * 1.25)).build(), new TransportResponseHandler<UnicastPingResponse>() {

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
                logger.trace("[{}] received response from {}: {}", id, nodeToSend, Arrays.toString(response.pingResponses));
                try {
                    DiscoveryNodes discoveryNodes = contextProvider.nodes();
                    for (PingResponse pingResponse : response.pingResponses) {
                        if (pingResponse.node().equals(discoveryNodes.getLocalNode())) {
                            // that's us, ignore
                            continue;
                        }
                        SendPingsHandler sendPingsHandler = receivedResponses.get(response.id);
                        if (sendPingsHandler == null) {
                            if (!closed) {
                                // Only log when we're not closing the node. Having no send ping handler is then expected
                                logger.warn("received ping response {} with no matching handler id [{}]", pingResponse, response.id);
                            }
                        } else {
                            sendPingsHandler.pingCollection().addPing(pingResponse);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void handleException(TransportException exp) {
                latch.countDown();
                if (exp instanceof ConnectTransportException) {
                    // ok, not connected...
                    logger.trace((Supplier<?>) () -> new ParameterizedMessage("failed to connect to {}", nodeToSend), exp);
                } else {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to send ping to [{}]", node), exp);
                }
            }
        });
    }

    private UnicastPingResponse handlePingRequest(final UnicastPingRequest request) {
        temporalResponses.add(request.pingResponse);
        threadPool.schedule(TimeValue.timeValueMillis(request.timeout.millis() * 2), ThreadPool.Names.SAME, new Runnable() {
            @Override
            public void run() {
                temporalResponses.remove(request.pingResponse);
            }
        });

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
