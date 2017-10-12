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

package org.elasticsearch.test.transport;

import com.carrotsearch.randomizedtesting.SysGlobals;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.tasks.MockTaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.MockTcpTransport;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportStats;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * A mock transport service that allows to simulate different network topology failures.
 * Internally it maps TransportAddress objects to rules that inject failures.
 * Adding rules for a node is done by adding rules for all bound addresses of a node
 * (and the publish address, if different).
 * Matching requests to rules is based on the transport address associated with the
 * discovery node of the request, namely by DiscoveryNode.getAddress().
 * This address is usually the publish address of the node but can also be a different one
 * (for example, @see org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing, which constructs
 * fake DiscoveryNode instances where the publish address is one of the bound addresses).
 */
public final class MockTransportService extends TransportService {

    private final Map<DiscoveryNode, List<Transport.Connection>> openConnections = new HashMap<>();
    private static final int JVM_ORDINAL = Integer.parseInt(System.getProperty(SysGlobals.CHILDVM_SYSPROP_JVM_ID, "0"));

    public static class TestPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING);
        }
    }

    public static MockTransportService createNewService(Settings settings, Version version, ThreadPool threadPool,
                                                        @Nullable ClusterSettings clusterSettings) {
        // some tests use MockTransportService to do network based testing. Yet, we run tests in multiple JVMs that means
        // concurrent tests could claim port that another JVM just released and if that test tries to simulate a disconnect it might
        // be smart enough to re-connect depending on what is tested. To reduce the risk, since this is very hard to debug we use
        // a different default port range per JVM unless the incoming settings override it
        int basePort = 10300 + (JVM_ORDINAL * 100); // use a non-default port otherwise some cluster in this JVM might reuse a port
        settings = Settings.builder().put(TcpTransport.PORT.getKey(), basePort + "-" + (basePort + 100)).put(settings).build();
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        final Transport transport = new MockTcpTransport(settings, threadPool, BigArrays.NON_RECYCLING_INSTANCE,
            new NoneCircuitBreakerService(), namedWriteableRegistry, new NetworkService(Collections.emptyList()), version);
        return createNewService(settings, transport, version, threadPool, clusterSettings);
    }

    public static MockTransportService createNewService(Settings settings, Transport transport, Version version, ThreadPool threadPool,
                                                        @Nullable ClusterSettings clusterSettings) {
        return new MockTransportService(settings, transport, threadPool, TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress ->
                new DiscoveryNode(Node.NODE_NAME_SETTING.get(settings), UUIDs.randomBase64UUID(), boundAddress.publishAddress(),
                    Node.NODE_ATTRIBUTES.getAsMap(settings), DiscoveryNode.getRolesFromSettings(settings), version),
            clusterSettings);
    }

    private final Transport original;

    /**
     * Build the service.
     *
     * @param clusterSettings if non null the {@linkplain TransportService} will register with the {@link ClusterSettings} for settings
     *                        updates for {@link #TRACE_LOG_EXCLUDE_SETTING} and {@link #TRACE_LOG_INCLUDE_SETTING}.
     */
    public MockTransportService(Settings settings, Transport transport, ThreadPool threadPool, TransportInterceptor interceptor,
                                @Nullable ClusterSettings clusterSettings) {
        this(settings, transport, threadPool, interceptor, (boundAddress) ->
            DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), settings.get(Node.NODE_NAME_SETTING.getKey(),
                UUIDs.randomBase64UUID())), clusterSettings);
    }

    /**
     * Build the service.
     *
     * @param clusterSettings if non null the {@linkplain TransportService} will register with the {@link ClusterSettings} for settings
     *                        updates for {@link #TRACE_LOG_EXCLUDE_SETTING} and {@link #TRACE_LOG_INCLUDE_SETTING}.
     */
    public MockTransportService(Settings settings, Transport transport, ThreadPool threadPool, TransportInterceptor interceptor,
                                Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
                                @Nullable ClusterSettings clusterSettings) {
        super(settings, new LookupTestTransport(transport), threadPool, interceptor, localNodeFactory, clusterSettings);
        this.original = transport;
    }

    public static TransportAddress[] extractTransportAddresses(TransportService transportService) {
        HashSet<TransportAddress> transportAddresses = new HashSet<>();
        BoundTransportAddress boundTransportAddress = transportService.boundAddress();
        transportAddresses.addAll(Arrays.asList(boundTransportAddress.boundAddresses()));
        transportAddresses.add(boundTransportAddress.publishAddress());
        return transportAddresses.toArray(new TransportAddress[transportAddresses.size()]);
    }

    @Override
    protected TaskManager createTaskManager() {
        if (MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING.get(settings)) {
            return new MockTaskManager(settings);
        } else {
            return super.createTaskManager();
        }
    }

    private volatile String executorName;

    public void setExecutorName(final String executorName) {
        this.executorName = executorName;
    }

    @Override
    protected ExecutorService getExecutorService() {
        return executorName == null ? super.getExecutorService() : getThreadPool().executor(executorName);
    }

    /**
     * Clears all the registered rules.
     */
    public void clearAllRules() {
        transport().transports.clear();
    }

    /**
     * Clears the rule associated with the provided transport service.
     */
    public void clearRule(TransportService transportService) {
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            clearRule(transportAddress);
        }
    }

    /**
     * Clears the rule associated with the provided transport address.
     */
    public void clearRule(TransportAddress transportAddress) {
        Transport transport = transport().transports.remove(transportAddress);
        if (transport instanceof ClearableTransport) {
            ((ClearableTransport) transport).clearRule();
        }
    }

    /**
     * Returns the original Transport service wrapped by this mock transport service.
     */
    public Transport original() {
        return original;
    }

    /**
     * Adds a rule that will cause every send request to fail, and each new connect since the rule
     * is added to fail as well.
     */
    public void addFailToSendNoConnectRule(TransportService transportService) {
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            addFailToSendNoConnectRule(transportAddress);
        }
    }

    /**
     * Adds a rule that will cause every send request to fail, and each new connect since the rule
     * is added to fail as well.
     */
    public void addFailToSendNoConnectRule(TransportAddress transportAddress) {
        addDelegate(transportAddress, new DelegateTransport(original) {

            @Override
            public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile,
                                      CheckedBiConsumer<Connection, ConnectionProfile, IOException> connectionValidator)
                throws ConnectTransportException {
                if (original.nodeConnected(node) == false) {
                    // connecting to an already connected node is a no-op
                    throw new ConnectTransportException(node, "DISCONNECT: simulated");
                }
            }

            @Override
            public Connection openConnection(DiscoveryNode node, ConnectionProfile profile) throws IOException {
                throw new ConnectTransportException(node, "DISCONNECT: simulated");
            }

            @Override
            protected void sendRequest(Connection connection, long requestId, String action, TransportRequest request,
                                       TransportRequestOptions options) throws IOException {
                connection.close();
                // send the request, which will blow up
                connection.sendRequest(requestId, action, request, options);
            }
        });
    }

    /**
     * Adds a rule that will cause matching operations to throw ConnectTransportExceptions
     */
    public void addFailToSendNoConnectRule(TransportService transportService, final String... blockedActions) {
        addFailToSendNoConnectRule(transportService, new HashSet<>(Arrays.asList(blockedActions)));
    }

    /**
     * Adds a rule that will cause matching operations to throw ConnectTransportExceptions
     */
    public void addFailToSendNoConnectRule(TransportAddress transportAddress, final String... blockedActions) {
        addFailToSendNoConnectRule(transportAddress, new HashSet<>(Arrays.asList(blockedActions)));
    }

    /**
     * Adds a rule that will cause matching operations to throw ConnectTransportExceptions
     */
    public void addFailToSendNoConnectRule(TransportService transportService, final Set<String> blockedActions) {
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            addFailToSendNoConnectRule(transportAddress, blockedActions);
        }
    }

    /**
     * Adds a rule that will cause matching operations to throw ConnectTransportExceptions
     */
    public void addFailToSendNoConnectRule(TransportAddress transportAddress, final Set<String> blockedActions) {

        addDelegate(transportAddress, new DelegateTransport(original) {

            @Override
            protected void sendRequest(Connection connection, long requestId, String action, TransportRequest request,
                                       TransportRequestOptions options) throws IOException {
                if (blockedActions.contains(action)) {
                    logger.info("--> preventing {} request", action);
                    connection.close();
                }
                connection.sendRequest(requestId, action, request, options);
            }
        });
    }

    /**
     * Adds a rule that will cause ignores each send request, simulating an unresponsive node
     * and failing to connect once the rule was added.
     */
    public void addUnresponsiveRule(TransportService transportService) {
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            addUnresponsiveRule(transportAddress);
        }
    }

    /**
     * Adds a rule that will cause ignores each send request, simulating an unresponsive node
     * and failing to connect once the rule was added.
     */
    public void addUnresponsiveRule(TransportAddress transportAddress) {
        addDelegate(transportAddress, new DelegateTransport(original) {

            @Override
            public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile,
                                      CheckedBiConsumer<Connection, ConnectionProfile, IOException> connectionValidator)
                throws ConnectTransportException {
                if (original.nodeConnected(node) == false) {
                    // connecting to an already connected node is a no-op
                    throw new ConnectTransportException(node, "UNRESPONSIVE: simulated");
                }
            }

            @Override
            public Connection openConnection(DiscoveryNode node, ConnectionProfile profile) throws IOException {
                throw new ConnectTransportException(node, "UNRESPONSIVE: simulated");
            }

            @Override
            protected void sendRequest(Connection connection, long requestId, String action, TransportRequest request,
                                       TransportRequestOptions options) throws IOException {
                // don't send anything, the receiving node is unresponsive
            }
        });
    }

    /**
     * Adds a rule that will cause ignores each send request, simulating an unresponsive node
     * and failing to connect once the rule was added.
     *
     * @param duration the amount of time to delay sending and connecting.
     */
    public void addUnresponsiveRule(TransportService transportService, final TimeValue duration) {
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            addUnresponsiveRule(transportAddress, duration);
        }
    }

    /**
     * Adds a rule that will cause ignores each send request, simulating an unresponsive node
     * and failing to connect once the rule was added.
     *
     * @param duration the amount of time to delay sending and connecting.
     */
    public void addUnresponsiveRule(TransportAddress transportAddress, final TimeValue duration) {
        final long startTime = System.currentTimeMillis();

        addDelegate(transportAddress, new ClearableTransport(original) {
            private final Queue<Runnable> requestsToSendWhenCleared = new LinkedBlockingDeque<>();
            private boolean cleared = false;

            TimeValue getDelay() {
                return new TimeValue(duration.millis() - (System.currentTimeMillis() - startTime));
            }

            @Override
            public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile,
                                      CheckedBiConsumer<Connection, ConnectionProfile, IOException> connectionValidator)
                throws ConnectTransportException {
                if (original.nodeConnected(node)) {
                    // connecting to an already connected node is a no-op
                    return;
                }
                TimeValue delay = getDelay();
                if (delay.millis() <= 0) {
                    original.connectToNode(node, connectionProfile, connectionValidator);
                    return;
                }

                // TODO: Replace with proper setting
                TimeValue connectingTimeout = TcpTransport.TCP_CONNECT_TIMEOUT.getDefault(Settings.EMPTY);
                try {
                    if (delay.millis() < connectingTimeout.millis()) {
                        Thread.sleep(delay.millis());
                        original.connectToNode(node, connectionProfile, connectionValidator);
                    } else {
                        Thread.sleep(connectingTimeout.millis());
                        throw new ConnectTransportException(node, "UNRESPONSIVE: simulated");
                    }
                } catch (InterruptedException e) {
                    throw new ConnectTransportException(node, "UNRESPONSIVE: simulated");
                }
            }

            @Override
            public Connection openConnection(DiscoveryNode node, ConnectionProfile profile) throws IOException {
                TimeValue delay = getDelay();
                if (delay.millis() <= 0) {
                    return original.openConnection(node, profile);
                }

                // TODO: Replace with proper setting
                TimeValue connectingTimeout = TcpTransport.TCP_CONNECT_TIMEOUT.getDefault(Settings.EMPTY);
                try {
                    if (delay.millis() < connectingTimeout.millis()) {
                        Thread.sleep(delay.millis());
                        return original.openConnection(node, profile);
                    } else {
                        Thread.sleep(connectingTimeout.millis());
                        throw new ConnectTransportException(node, "UNRESPONSIVE: simulated");
                    }
                } catch (InterruptedException e) {
                    throw new ConnectTransportException(node, "UNRESPONSIVE: simulated");
                }
            }

            @Override
            protected void sendRequest(Connection connection, long requestId, String action, TransportRequest request,
                                       TransportRequestOptions options) throws IOException {
                // delayed sending - even if larger then the request timeout to simulated a potential late response from target node
                TimeValue delay = getDelay();
                if (delay.millis() <= 0) {
                    connection.sendRequest(requestId, action, request, options);
                    return;
                }

                // poor mans request cloning...
                RequestHandlerRegistry reg = MockTransportService.this.getRequestHandler(action);
                BytesStreamOutput bStream = new BytesStreamOutput();
                request.writeTo(bStream);
                final TransportRequest clonedRequest = reg.newRequest();
                clonedRequest.readFrom(bStream.bytes().streamInput());

                Runnable runnable = new AbstractRunnable() {
                    AtomicBoolean requestSent = new AtomicBoolean();

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug("failed to send delayed request", e);
                    }

                    @Override
                    protected void doRun() throws IOException {
                        if (requestSent.compareAndSet(false, true)) {
                            connection.sendRequest(requestId, action, clonedRequest, options);
                        }
                    }
                };

                // store the request to send it once the rule is cleared.
                synchronized (this) {
                    if (cleared) {
                        runnable.run();
                    } else {
                        requestsToSendWhenCleared.add(runnable);
                        threadPool.schedule(delay, ThreadPool.Names.GENERIC, runnable);
                    }
                }
            }

            @Override
            public void clearRule() {
                synchronized (this) {
                    assert cleared == false;
                    cleared = true;
                    requestsToSendWhenCleared.forEach(Runnable::run);
                }
            }
        });
    }

    /**
     * Adds a new delegate transport that is used for communication with the given transport service.
     *
     * @return <tt>true</tt> iff no other delegate was registered for any of the addresses bound by transport service.
     */
    public boolean addDelegate(TransportService transportService, DelegateTransport transport) {
        boolean noRegistered = true;
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            noRegistered &= addDelegate(transportAddress, transport);
        }
        return noRegistered;
    }

    /**
     * Adds a new delegate transport that is used for communication with the given transport address.
     *
     * @return <tt>true</tt> iff no other delegate was registered for this address before.
     */
    public boolean addDelegate(TransportAddress transportAddress, DelegateTransport transport) {
        return transport().transports.put(transportAddress, transport) == null;
    }

    private LookupTestTransport transport() {
        return (LookupTestTransport) transport;
    }

    /**
     * A lookup transport that has a list of potential Transport implementations to delegate to for node operations,
     * if none is registered, then the default one is used.
     */
    private static class LookupTestTransport extends DelegateTransport {

        final ConcurrentMap<TransportAddress, Transport> transports = ConcurrentCollections.newConcurrentMap();

        LookupTestTransport(Transport transport) {
            super(transport);
        }

        private Transport getTransport(DiscoveryNode node) {
            Transport transport = transports.get(node.getAddress());
            if (transport != null) {
                return transport;
            }
            return this.transport;
        }

        @Override
        public boolean nodeConnected(DiscoveryNode node) {
            return getTransport(node).nodeConnected(node);
        }

        @Override
        public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile,
                                  CheckedBiConsumer<Connection, ConnectionProfile, IOException> connectionValidator)
            throws ConnectTransportException {
            getTransport(node).connectToNode(node, connectionProfile, connectionValidator);
        }

        @Override
        public void disconnectFromNode(DiscoveryNode node) {
            getTransport(node).disconnectFromNode(node);
        }

        @Override
        public Connection getConnection(DiscoveryNode node) {
            return getTransport(node).getConnection(node);
        }

        @Override
        public Connection openConnection(DiscoveryNode node, ConnectionProfile profile) throws IOException {
            return getTransport(node).openConnection(node, profile);
        }
    }

    /**
     * A pure delegate transport.
     * Can be extracted to a common class if needed in other places in the codebase.
     */
    public static class DelegateTransport implements Transport {

        protected final Transport transport;


        public DelegateTransport(Transport transport) {
            this.transport = transport;
        }

        @Override
        public void setTransportService(TransportService service) {
            transport.setTransportService(service);
        }

        @Override
        public BoundTransportAddress boundAddress() {
            return transport.boundAddress();
        }

        @Override
        public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws UnknownHostException {
            return transport.addressesFromString(address, perAddressLimit);
        }

        @Override
        public boolean nodeConnected(DiscoveryNode node) {
            return transport.nodeConnected(node);
        }

        @Override
        public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile,
                                  CheckedBiConsumer<Connection, ConnectionProfile, IOException> connectionValidator)
            throws ConnectTransportException {
            transport.connectToNode(node, connectionProfile, connectionValidator);
        }

        @Override
        public void disconnectFromNode(DiscoveryNode node) {
            transport.disconnectFromNode(node);
        }

        @Override
        public List<String> getLocalAddresses() {
            return transport.getLocalAddresses();
        }

        @Override
        public long newRequestId() {
            return transport.newRequestId();
        }

        @Override
        public Connection getConnection(DiscoveryNode node) {
            return new FilteredConnection(transport.getConnection(node)) {
                @Override
                public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                    throws IOException, TransportException {
                    DelegateTransport.this.sendRequest(connection, requestId, action, request, options);
                }
            };
        }

        @Override
        public Connection openConnection(DiscoveryNode node, ConnectionProfile profile) throws IOException {
            return new FilteredConnection(transport.openConnection(node, profile)) {
                @Override
                public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                    throws IOException, TransportException {
                    DelegateTransport.this.sendRequest(connection, requestId, action, request, options);
                }
            };
        }

        @Override
        public TransportStats getStats() {
            return transport.getStats();
        }

        @Override
        public Lifecycle.State lifecycleState() {
            return transport.lifecycleState();
        }

        @Override
        public void addLifecycleListener(LifecycleListener listener) {
            transport.addLifecycleListener(listener);
        }

        @Override
        public void removeLifecycleListener(LifecycleListener listener) {
            transport.removeLifecycleListener(listener);
        }

        @Override
        public void start() {
            transport.start();
        }

        @Override
        public void stop() {
            transport.stop();
        }

        @Override
        public void close() {
            transport.close();
        }

        @Override
        public Map<String, BoundTransportAddress> profileBoundAddresses() {
            return transport.profileBoundAddresses();
        }

        protected void sendRequest(Transport.Connection connection, long requestId, String action, TransportRequest request,
                                   TransportRequestOptions options) throws IOException {
            connection.sendRequest(requestId, action, request, options);
        }
    }

    /**
     * The delegate transport instances defined in this class mock various kinds of disruption types. This subclass adds a method
     * {@link #clearRule()} so that when the disruptions are cleared (see {@link #clearRule(TransportService)}) this gives the
     * disruption a possibility to run clean-up actions.
     */
    public abstract static class ClearableTransport extends DelegateTransport {

        public ClearableTransport(Transport transport) {
            super(transport);
        }

        /**
         * Called by {@link #clearRule(TransportService)}
         */
        public abstract void clearRule();
    }


    List<Tracer> activeTracers = new CopyOnWriteArrayList<>();

    public static class Tracer {
        public void receivedRequest(long requestId, String action) {
        }

        public void responseSent(long requestId, String action) {
        }

        public void responseSent(long requestId, String action, Throwable t) {
        }

        public void receivedResponse(long requestId, DiscoveryNode sourceNode, String action) {
        }

        public void requestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
        }
    }

    public void addTracer(Tracer tracer) {
        activeTracers.add(tracer);
    }

    public boolean removeTracer(Tracer tracer) {
        return activeTracers.remove(tracer);
    }

    public void clearTracers() {
        activeTracers.clear();
    }

    @Override
    protected boolean traceEnabled() {
        return super.traceEnabled() || activeTracers.isEmpty() == false;
    }

    @Override
    protected void traceReceivedRequest(long requestId, String action) {
        super.traceReceivedRequest(requestId, action);
        for (Tracer tracer : activeTracers) {
            tracer.receivedRequest(requestId, action);
        }
    }

    @Override
    protected void traceResponseSent(long requestId, String action) {
        super.traceResponseSent(requestId, action);
        for (Tracer tracer : activeTracers) {
            tracer.responseSent(requestId, action);
        }
    }

    @Override
    protected void traceResponseSent(long requestId, String action, Exception e) {
        super.traceResponseSent(requestId, action, e);
        for (Tracer tracer : activeTracers) {
            tracer.responseSent(requestId, action, e);
        }
    }

    @Override
    protected void traceReceivedResponse(long requestId, DiscoveryNode sourceNode, String action) {
        super.traceReceivedResponse(requestId, sourceNode, action);
        for (Tracer tracer : activeTracers) {
            tracer.receivedResponse(requestId, sourceNode, action);
        }
    }

    @Override
    protected void traceRequestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
        super.traceRequestSent(node, requestId, action, options);
        for (Tracer tracer : activeTracers) {
            tracer.requestSent(node, requestId, action, options);
        }
    }

    private static class FilteredConnection implements Transport.Connection {
        protected final Transport.Connection connection;

        private FilteredConnection(Transport.Connection connection) {
            this.connection = connection;
        }

        @Override
        public DiscoveryNode getNode() {
            return connection.getNode();
        }

        @Override
        public Version getVersion() {
            return connection.getVersion();
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws IOException, TransportException {
            connection.sendRequest(requestId, action, request, options);
        }

        @Override
        public void close() throws IOException {
            connection.close();
        }

        @Override
        public Object getCacheKey() {
            return connection.getCacheKey();
        }
    }

    public Transport getOriginalTransport() {
        Transport transport = transport();
        while (transport instanceof DelegateTransport) {
            transport = ((DelegateTransport) transport).transport;
        }
        return transport;
    }

    @Override
    public Transport.Connection openConnection(DiscoveryNode node, ConnectionProfile profile) throws IOException {
        FilteredConnection filteredConnection = new FilteredConnection(super.openConnection(node, profile)) {
            final AtomicBoolean closed = new AtomicBoolean(false);

            @Override
            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    if (closed.compareAndSet(false, true)) {
                        synchronized (openConnections) {
                            List<Transport.Connection> connections = openConnections.get(node);
                            boolean remove = connections.remove(this);
                            assert remove;
                            if (connections.isEmpty()) {
                                openConnections.remove(node);
                            }
                        }
                    }
                }

            }
        };
        synchronized (openConnections) {
            List<Transport.Connection> connections = openConnections.computeIfAbsent(node,
                (n) -> new CopyOnWriteArrayList<>());
            connections.add(filteredConnection);
        }
        return filteredConnection;
    }

    @Override
    protected void doClose() throws IOException {
        super.doClose();
        synchronized (openConnections) {
            assert openConnections.size() == 0 : "still open connections: " + openConnections;
        }
    }

    public DiscoveryNode getLocalDiscoNode() {
        return this.getLocalNode();
    }
}
