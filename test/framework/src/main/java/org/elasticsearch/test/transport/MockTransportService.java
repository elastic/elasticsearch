/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeRequestHelper;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.tasks.MockTaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ClusterConnectionManager;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.elasticsearch.transport.netty4.SharedGroupFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

/**
 * A mock delegate service that allows to simulate different network topology failures.
 * Internally it maps TransportAddress objects to rules that inject failures.
 * Adding rules for a node is done by adding rules for all bound addresses of a node
 * (and the publish address, if different).
 * Matching requests to rules is based on the delegate address associated with the
 * discovery node of the request, namely by DiscoveryNode.getAddress().
 * This address is usually the publish address of the node but can also be a different one
 * (for example, @see org.elasticsearch.discovery.HandshakingTransportAddressConnector, which constructs
 * fake DiscoveryNode instances where the publish address is one of the bound addresses).
 */
public class MockTransportService extends TransportService {
    private static final Logger logger = LogManager.getLogger(MockTransportService.class);

    private final Map<DiscoveryNode, List<Transport.Connection>> openConnections = new HashMap<>();

    private final List<Runnable> onStopListeners = new CopyOnWriteArrayList<>();
    private final AtomicReference<Consumer<Transport.Connection>> onConnectionClosedCallback = new AtomicReference<>();

    private final DelegatingTransportMessageListener messageListener = new DelegatingTransportMessageListener();

    public static class TestPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING, MockTaskManager.SPY_TASK_MANAGER_SETTING);
        }
    }

    public static MockTransportService createNewService(
        Settings settings,
        VersionInformation version,
        TransportVersion transportVersion,
        ThreadPool threadPool
    ) {
        return createNewService(settings, version, transportVersion, threadPool, null);
    }

    public static MockTransportService createNewService(
        Settings settings,
        VersionInformation version,
        TransportVersion transportVersion,
        ThreadPool threadPool,
        @Nullable ClusterSettings clusterSettings
    ) {
        return createNewService(
            settings,
            newMockTransport(settings, transportVersion, threadPool),
            version,
            threadPool,
            clusterSettings,
            Collections.emptySet()
        );
    }

    public static TcpTransport newMockTransport(Settings settings, TransportVersion version, ThreadPool threadPool) {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, List.of());
        var namedWriteables = CollectionUtils.concatLists(searchModule.getNamedWriteables(), ClusterModule.getNamedWriteables());
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
        return newMockTransport(settings, version, threadPool, namedWriteableRegistry);
    }

    public static TcpTransport newMockTransport(
        Settings settings,
        TransportVersion version,
        ThreadPool threadPool,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        settings = Settings.builder().put(TransportSettings.PORT.getKey(), ESTestCase.getPortRange()).put(settings).build();
        return new Netty4Transport(
            settings,
            version,
            threadPool,
            new NetworkService(Collections.emptyList()),
            new MockPageCacheRecycler(settings),
            namedWriteableRegistry,
            new NoneCircuitBreakerService(),
            new SharedGroupFactory(settings)
        );
    }

    public static MockTransportService createNewService(
        Settings settings,
        Transport transport,
        VersionInformation version,
        ThreadPool threadPool,
        @Nullable ClusterSettings clusterSettings,
        Set<String> taskHeaders
    ) {
        return createNewService(settings, transport, version, threadPool, clusterSettings, taskHeaders, NOOP_TRANSPORT_INTERCEPTOR);
    }

    public static MockTransportService createNewService(
        Settings settings,
        Transport transport,
        VersionInformation version,
        ThreadPool threadPool,
        @Nullable ClusterSettings clusterSettings,
        Set<String> taskHeaders,
        TransportInterceptor interceptor
    ) {
        return new MockTransportService(
            settings,
            new StubbableTransport(transport),
            threadPool,
            interceptor,
            boundAddress -> DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID())
                .name(Node.NODE_NAME_SETTING.get(settings))
                .address(boundAddress.publishAddress())
                .attributes(Node.NODE_ATTRIBUTES.getAsMap(settings))
                .roles(DiscoveryNode.getRolesFromSettings(settings))
                .version(version)
                .build(),
            clusterSettings,
            createTaskManager(settings, threadPool, taskHeaders, Tracer.NOOP)
        );
    }

    public static MockTransportService getInstance(String nodeName) {
        assertNotNull("nodeName must not be null", nodeName);
        return ESTestCase.asInstanceOf(
            MockTransportService.class,
            ESIntegTestCase.internalCluster().getInstance(TransportService.class, nodeName)
        );
    }

    private final Transport original;
    private final EsThreadPoolExecutor testExecutor;

    /**
     * Build the service.
     *
     * @param clusterSettings if non null the {@linkplain TransportService} will register with the {@link ClusterSettings} for settings
     *                        updates for {@link TransportSettings#TRACE_LOG_EXCLUDE_SETTING} and
     *                        {@link TransportSettings#TRACE_LOG_INCLUDE_SETTING}.
     */
    public MockTransportService(
        Settings settings,
        Transport transport,
        ThreadPool threadPool,
        TransportInterceptor interceptor,
        @Nullable ClusterSettings clusterSettings
    ) {
        this(
            settings,
            new StubbableTransport(transport),
            threadPool,
            interceptor,
            (boundAddress) -> DiscoveryNodeUtils.builder(settings.get(Node.NODE_NAME_SETTING.getKey(), UUIDs.randomBase64UUID()))
                .applySettings(settings)
                .address(boundAddress.publishAddress())
                .build(),
            clusterSettings,
            createTaskManager(settings, threadPool, Set.of(), Tracer.NOOP)
        );
    }

    /**
     * Build the service.
     *
     * @param clusterSettings if non null the {@linkplain TransportService} will register with the {@link ClusterSettings} for settings
     *                        updates for {@link TransportSettings#TRACE_LOG_EXCLUDE_SETTING} and
     *                        {@link TransportSettings#TRACE_LOG_INCLUDE_SETTING}.
     */
    public MockTransportService(
        Settings settings,
        Transport transport,
        ThreadPool threadPool,
        TransportInterceptor interceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        @Nullable ClusterSettings clusterSettings,
        Set<String> taskHeaders
    ) {
        this(
            settings,
            new StubbableTransport(transport),
            threadPool,
            interceptor,
            localNodeFactory,
            clusterSettings,
            createTaskManager(settings, threadPool, taskHeaders, Tracer.NOOP)
        );
    }

    public MockTransportService(
        Settings settings,
        Transport transport,
        ThreadPool threadPool,
        TransportInterceptor interceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        @Nullable ClusterSettings clusterSettings
    ) {
        this(
            settings,
            new StubbableTransport(transport),
            threadPool,
            interceptor,
            localNodeFactory,
            clusterSettings,
            createTaskManager(settings, threadPool, Set.of(), Tracer.NOOP)
        );
    }

    private MockTransportService(
        Settings settings,
        StubbableTransport transport,
        ThreadPool threadPool,
        TransportInterceptor interceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        @Nullable ClusterSettings clusterSettings,
        TaskManager taskManager
    ) {
        super(
            settings,
            transport,
            threadPool,
            interceptor,
            localNodeFactory,
            clusterSettings,
            new StubbableConnectionManager(new ClusterConnectionManager(settings, transport, threadPool.getThreadContext())),
            taskManager,
            Tracer.NOOP
        );
        this.original = transport.getDelegate();
        this.testExecutor = EsExecutors.newScaling(
            "mock-transport",
            1,
            4,
            30,
            TimeUnit.SECONDS,
            true,
            EsExecutors.daemonThreadFactory("mock-transport"),
            threadPool.getThreadContext()
        );
    }

    private static TransportAddress[] extractTransportAddresses(TransportService transportService) {
        HashSet<TransportAddress> transportAddresses = new HashSet<>();
        BoundTransportAddress boundTransportAddress = transportService.boundAddress();
        transportAddresses.addAll(Arrays.asList(boundTransportAddress.boundAddresses()));
        transportAddresses.add(boundTransportAddress.publishAddress());
        return transportAddresses.toArray(new TransportAddress[transportAddresses.size()]);
    }

    public static TaskManager createTaskManager(Settings settings, ThreadPool threadPool, Set<String> taskHeaders, Tracer tracer) {
        if (MockTaskManager.SPY_TASK_MANAGER_SETTING.get(settings)) {
            return spy(createMockTaskManager(settings, threadPool, taskHeaders, tracer));
        } else {
            return createMockTaskManager(settings, threadPool, taskHeaders, tracer);
        }
    }

    private static TaskManager createMockTaskManager(Settings settings, ThreadPool threadPool, Set<String> taskHeaders, Tracer tracer) {
        if (MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING.get(settings)) {
            return new MockTaskManager(settings, threadPool, taskHeaders);
        } else {
            return new TaskManager(settings, threadPool, taskHeaders, tracer);
        }
    }

    /**
     * Clears all the registered rules.
     */
    public void clearAllRules() {
        transport().clearBehaviors();
        connectionManager().clearBehaviors();
    }

    /**
     * Clears all the inbound rules.
     */
    public void clearInboundRules() {
        transport().clearInboundBehaviors();
    }

    /**
     * Clears the outbound rules associated with the provided delegate service.
     */
    public void clearOutboundRules(TransportService transportService) {
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            clearOutboundRules(transportAddress);
        }
    }

    /**
     * Clears the outbound rules associated with the provided delegate address.
     */
    public void clearOutboundRules(TransportAddress transportAddress) {
        transport().clearOutboundBehaviors(transportAddress);
        connectionManager().clearBehavior(transportAddress);
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
        transport().addConnectBehavior(
            transportAddress,
            (transport, discoveryNode, profile, listener) -> listener.onFailure(
                new ConnectTransportException(discoveryNode, "DISCONNECT: simulated")
            )
        );

        transport().addSendBehavior(transportAddress, (connection, requestId, action, request, options) -> {
            connection.close();
            // send the request, which will blow up
            connection.sendRequest(requestId, action, request, options);
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
    public void addFailToSendNoConnectRule(TransportService transportService, final Set<String> blockedActions) {
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            addFailToSendNoConnectRule(transportAddress, blockedActions);
        }
    }

    /**
     * Adds a rule that will cause matching operations to throw ConnectTransportExceptions
     */
    public void addFailToSendNoConnectRule(TransportAddress transportAddress, final Set<String> blockedActions) {
        transport().addSendBehavior(transportAddress, (connection, requestId, action, request, options) -> {
            if (blockedActions.contains(action)) {
                logger.info("--> preventing {} request", action);
                connection.close();
            }
            connection.sendRequest(requestId, action, request, options);
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
        transport().addConnectBehavior(
            transportAddress,
            (transport, discoveryNode, profile, listener) -> listener.onFailure(
                new ConnectTransportException(discoveryNode, "UNRESPONSIVE: simulated")
            )
        );

        transport().addSendBehavior(transportAddress, new StubbableTransport.SendRequestBehavior() {

            private final Set<Transport.Connection> toClose = ConcurrentHashMap.newKeySet();
            private final RefCounted refs = AbstractRefCounted.of(this::closeConnections);

            @Override
            public void sendRequest(
                Transport.Connection connection,
                long requestId,
                String action,
                TransportRequest request,
                TransportRequestOptions options
            ) throws IOException {
                if (connection.isClosed()) {
                    throw new NodeNotConnectedException(connection.getNode(), "connection already closed");
                } else if (refs.tryIncRef()) {
                    // don't send anything, the receiving node is unresponsive
                    toClose.add(connection);
                    refs.decRef();
                } else {
                    connection.sendRequest(requestId, action, request, options);
                }
            }

            @Override
            public void clearCallback() {
                // close to simulate that tcp-ip eventually times out and closes connection (necessary to ensure transport eventually
                // responds).
                refs.decRef();
            }

            private void closeConnections() {
                // close to simulate that tcp-ip eventually times out and closes connection (necessary to ensure transport eventually
                // responds).
                try {
                    IOUtils.close(toClose);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
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
        final long startTimeInMillis = threadPool.relativeTimeInMillis();

        Supplier<TimeValue> delaySupplier = () -> {
            long elapsed = threadPool.relativeTimeInMillis() - startTimeInMillis;
            return new TimeValue(Math.max(duration.millis() - elapsed, 0));
        };

        transport().addConnectBehavior(transportAddress, new StubbableTransport.OpenConnectionBehavior() {
            private CountDownLatch stopLatch = new CountDownLatch(1);

            @Override
            public void openConnection(
                Transport transport,
                DiscoveryNode discoveryNode,
                ConnectionProfile profile,
                ActionListener<Transport.Connection> listener
            ) {
                TimeValue delay = delaySupplier.get();
                if (delay.millis() <= 0) {
                    original.openConnection(discoveryNode, profile, listener);
                    return;
                }

                // TODO: Replace with proper setting
                TimeValue connectingTimeout = TransportSettings.CONNECT_TIMEOUT.getDefault(Settings.EMPTY);
                try {
                    if (delay.millis() < connectingTimeout.millis()) {
                        stopLatch.await(delay.millis(), TimeUnit.MILLISECONDS);
                        original.openConnection(discoveryNode, profile, listener);
                    } else {
                        stopLatch.await(connectingTimeout.millis(), TimeUnit.MILLISECONDS);
                        listener.onFailure(new ConnectTransportException(discoveryNode, "UNRESPONSIVE: simulated"));
                    }
                } catch (InterruptedException e) {
                    listener.onFailure(new ConnectTransportException(discoveryNode, "UNRESPONSIVE: simulated"));
                }
            }

            @Override
            public void clearCallback() {
                stopLatch.countDown();
            }
        });

        transport().addSendBehavior(transportAddress, new StubbableTransport.SendRequestBehavior() {
            private final Queue<Runnable> requestsToSendWhenCleared = new LinkedBlockingDeque<>();
            private boolean cleared = false;

            @Override
            public void sendRequest(
                Transport.Connection connection,
                long requestId,
                String action,
                TransportRequest request,
                TransportRequestOptions options
            ) throws IOException {
                // delayed sending - even if larger then the request timeout to simulated a potential late response from target node
                TimeValue delay = delaySupplier.get();
                if (delay.millis() <= 0) {
                    connection.sendRequest(requestId, action, request, options);
                    return;
                }

                // poor mans request cloning...
                BytesStreamOutput bStream = new BytesStreamOutput();
                request.writeTo(bStream);
                RequestHandlerRegistry<?> reg = MockTransportService.this.getRequestHandler(action);
                final TransportRequest clonedRequest = reg.newRequest(bStream.bytes().streamInput());
                assert clonedRequest.getClass().equals(MasterNodeRequestHelper.unwrapTermOverride(request).getClass())
                    : clonedRequest + " vs " + request;

                final RunOnce runnable = new RunOnce(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        logger.debug(
                            () -> Strings.format(
                                "[%d][%s] failed to send delayed request to node [%s]",
                                requestId,
                                action,
                                connection.getNode()
                            ),
                            e
                        );
                        handleInternalSendException(action, connection.getNode(), requestId, null, e);
                    }

                    @Override
                    protected void doRun() throws IOException {
                        logger.debug(
                            () -> Strings.format("[%d][%s] sending delayed request to node [%s]", requestId, action, connection.getNode())
                        );
                        connection.sendRequest(requestId, action, clonedRequest, options);
                    }
                });

                // store the request to send it once the rule is cleared.
                synchronized (this) {
                    if (cleared) {
                        runnable.run();
                    } else {
                        requestsToSendWhenCleared.add(runnable);
                        logger.debug(
                            () -> Strings.format(
                                "[%d][%s] delaying sending request to node [%s] by [%s]",
                                requestId,
                                action,
                                connection.getNode(),
                                delay
                            )
                        );
                        threadPool.schedule(runnable, delay, testExecutor);
                    }
                }
            }

            @Override
            public void clearCallback() {
                synchronized (this) {
                    assert cleared == false;
                    cleared = true;
                    requestsToSendWhenCleared.forEach(Runnable::run);
                }
            }
        });
    }

    /**
     * Adds a new handling behavior that is used when the defined request is received.
     *
     */
    public <R extends TransportRequest> void addRequestHandlingBehavior(
        String actionName,
        StubbableTransport.RequestHandlingBehavior<R> handlingBehavior
    ) {
        transport().addRequestHandlingBehavior(actionName, handlingBehavior);
    }

    /**
     * Adds a new send behavior that is used for communication with the given delegate service.
     *
     * @return {@code true} if no other send behavior was registered for any of the addresses bound by delegate service.
     */
    public boolean addSendBehavior(TransportService transportService, StubbableTransport.SendRequestBehavior sendBehavior) {
        boolean noRegistered = true;
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            noRegistered &= addSendBehavior(transportAddress, sendBehavior);
        }
        return noRegistered;
    }

    /**
     * Adds a new send behavior that is used for communication with the given delegate address.
     *
     * @return {@code true} if no other send behavior was registered for this address before.
     */
    public boolean addSendBehavior(TransportAddress transportAddress, StubbableTransport.SendRequestBehavior sendBehavior) {
        return transport().addSendBehavior(transportAddress, sendBehavior);
    }

    /**
     * Adds a send behavior that is the default send behavior.
     *
     * @return {@code true} if no default send behavior was registered
     */
    public boolean addSendBehavior(StubbableTransport.SendRequestBehavior behavior) {
        return transport().setDefaultSendBehavior(behavior);
    }

    /**
     * Adds a new connect behavior that is used for creating connections with the given delegate service.
     *
     * @return {@code true} if no other send behavior was registered for any of the addresses bound by delegate service.
     */
    public boolean addConnectBehavior(TransportService transportService, StubbableTransport.OpenConnectionBehavior connectBehavior) {
        boolean noRegistered = true;
        for (TransportAddress transportAddress : extractTransportAddresses(transportService)) {
            noRegistered &= addConnectBehavior(transportAddress, connectBehavior);
        }
        return noRegistered;
    }

    /**
     * Adds a new connect behavior that is used for creating connections with the given delegate address.
     *
     * @return {@code true} if no other send behavior was registered for this address before.
     */
    public boolean addConnectBehavior(TransportAddress transportAddress, StubbableTransport.OpenConnectionBehavior connectBehavior) {
        return transport().addConnectBehavior(transportAddress, connectBehavior);
    }

    /**
     * Adds a get connection behavior that is used for communication with the given delegate address.
     *
     * @return {@code true} if no other get connection behavior was registered for this address before.
     */
    public boolean addGetConnectionBehavior(TransportAddress transportAddress, StubbableConnectionManager.GetConnectionBehavior behavior) {
        return connectionManager().addGetConnectionBehavior(transportAddress, behavior);
    }

    /**
     * Adds a get connection behavior that is the default get connection behavior.
     *
     * @return {@code true} if no default get connection behavior was registered.
     */
    public boolean addGetConnectionBehavior(StubbableConnectionManager.GetConnectionBehavior behavior) {
        return connectionManager().setDefaultGetConnectionBehavior(behavior);
    }

    /**
     * Adds a node connected behavior that is the default node connected behavior.
     *
     * @return {@code true} if no default node connected behavior was registered.
     */
    public boolean addNodeConnectedBehavior(StubbableConnectionManager.NodeConnectedBehavior behavior) {
        return connectionManager().setDefaultNodeConnectedBehavior(behavior);
    }

    public StubbableTransport transport() {
        return (StubbableTransport) transport;
    }

    public StubbableConnectionManager connectionManager() {
        return (StubbableConnectionManager) connectionManager;
    }

    @SuppressWarnings("resource") // Close is handled elsewhere
    public Transport getOriginalTransport() {
        Transport transport = transport();
        while (transport instanceof StubbableTransport) {
            transport = ((StubbableTransport) transport).getDelegate();
        }
        return transport;
    }

    @Override
    public void openConnection(DiscoveryNode node, ConnectionProfile connectionProfile, ActionListener<Transport.Connection> listener) {
        super.openConnection(node, connectionProfile, listener.safeMap(connection -> {
            synchronized (openConnections) {
                openConnections.computeIfAbsent(node, n -> new CopyOnWriteArrayList<>()).add(connection);
                connection.addCloseListener(ActionListener.running(() -> {
                    synchronized (openConnections) {
                        List<Transport.Connection> connections = openConnections.get(node);
                        boolean remove = connections.remove(connection);
                        assert remove : "Should have removed connection";
                        if (connections.isEmpty()) {
                            openConnections.remove(node);
                        }
                        if (openConnections.isEmpty()) {
                            openConnections.notifyAll();
                        }
                    }
                }));
            }
            return connection;
        }));
    }

    public void setOnConnectionClosedCallback(Consumer<Transport.Connection> callback) {
        onConnectionClosedCallback.set(callback);
    }

    @Override
    public void onConnectionClosed(Transport.Connection connection) {
        final Consumer<Transport.Connection> callback = onConnectionClosedCallback.get();
        if (callback != null) {
            callback.accept(connection);
        }
        super.onConnectionClosed(connection);
    }

    public void addOnStopListener(Runnable listener) {
        onStopListeners.add(listener);
    }

    @Override
    protected void doStop() {
        onStopListeners.forEach(Runnable::run);
        super.doStop();
    }

    @Override
    protected void doClose() throws IOException {
        super.doClose();
        try {
            synchronized (openConnections) {
                if (openConnections.isEmpty() == false) {
                    openConnections.wait(TimeUnit.SECONDS.toMillis(30L));
                }
                assert openConnections.size() == 0 : "still open connections: " + openConnections;
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } finally {
            assertTrue(ThreadPool.terminate(testExecutor, 10, TimeUnit.SECONDS));
        }
    }

    @Override
    public void onRequestReceived(long requestId, String action) {
        super.onRequestReceived(requestId, action);
        messageListener.onRequestReceived(requestId, action);
    }

    @Override
    public void onRequestSent(
        DiscoveryNode node,
        long requestId,
        String action,
        TransportRequest request,
        TransportRequestOptions options
    ) {
        super.onRequestSent(node, requestId, action, request, options);
        messageListener.onRequestSent(node, requestId, action, request, options);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void onResponseReceived(long requestId, Transport.ResponseContext holder) {
        super.onResponseReceived(requestId, holder);
        messageListener.onResponseReceived(requestId, holder);
    }

    @Override
    public void onResponseSent(long requestId, String action) {
        super.onResponseSent(requestId, action);
        messageListener.onResponseSent(requestId, action);
    }

    @Override
    public void onResponseSent(long requestId, String action, Exception e) {
        super.onResponseSent(requestId, action, e);
        messageListener.onResponseSent(requestId, action, e);
    }

    public void addMessageListener(TransportMessageListener listener) {
        messageListener.listeners.add(listener);
    }

    public void removeMessageListener(TransportMessageListener listener) {
        messageListener.listeners.remove(listener);
    }

    private static final class DelegatingTransportMessageListener implements TransportMessageListener {

        private final List<TransportMessageListener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void onRequestReceived(long requestId, String action) {
            for (TransportMessageListener listener : listeners) {
                listener.onRequestReceived(requestId, action);
            }
        }

        @Override
        public void onResponseSent(long requestId, String action) {
            for (TransportMessageListener listener : listeners) {
                listener.onResponseSent(requestId, action);
            }
        }

        @Override
        public void onResponseSent(long requestId, String action, Exception error) {
            for (TransportMessageListener listener : listeners) {
                listener.onResponseSent(requestId, action, error);
            }
        }

        @Override
        public void onRequestSent(
            DiscoveryNode node,
            long requestId,
            String action,
            TransportRequest request,
            TransportRequestOptions finalOptions
        ) {
            for (TransportMessageListener listener : listeners) {
                listener.onRequestSent(node, requestId, action, request, finalOptions);
            }
        }

        @Override
        @SuppressWarnings("rawtypes")
        public void onResponseReceived(long requestId, Transport.ResponseContext holder) {
            for (TransportMessageListener listener : listeners) {
                listener.onResponseReceived(requestId, holder);
            }
        }
    }
}
