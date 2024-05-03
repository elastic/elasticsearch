/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.network;

import org.elasticsearch.action.support.replication.ReplicationTask;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.http.HttpPreRequest;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer.ResyncTask;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.tasks.RawTaskStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * A module to handle registering and binding all network related classes.
 */
public final class NetworkModule {

    public static final String TRANSPORT_TYPE_KEY = "transport.type";
    public static final String HTTP_TYPE_KEY = "http.type";
    public static final String HTTP_TYPE_DEFAULT_KEY = "http.type.default";
    public static final String TRANSPORT_TYPE_DEFAULT_KEY = "transport.type.default";

    public static final Setting<String> TRANSPORT_DEFAULT_TYPE_SETTING = Setting.simpleString(
        TRANSPORT_TYPE_DEFAULT_KEY,
        Property.NodeScope
    );
    public static final Setting<String> HTTP_DEFAULT_TYPE_SETTING = Setting.simpleString(HTTP_TYPE_DEFAULT_KEY, Property.NodeScope);
    public static final Setting<String> HTTP_TYPE_SETTING = Setting.simpleString(HTTP_TYPE_KEY, Property.NodeScope);
    public static final Setting<String> TRANSPORT_TYPE_SETTING = Setting.simpleString(TRANSPORT_TYPE_KEY, Property.NodeScope);

    private final Settings settings;
    private final boolean transportClient;

    private static final List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
    private static final List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>();

    static {
        registerAllocationCommand(
            CancelAllocationCommand::new,
            CancelAllocationCommand::fromXContent,
            CancelAllocationCommand.COMMAND_NAME_FIELD
        );
        registerAllocationCommand(
            MoveAllocationCommand::new,
            MoveAllocationCommand::fromXContent,
            MoveAllocationCommand.COMMAND_NAME_FIELD
        );
        registerAllocationCommand(
            AllocateReplicaAllocationCommand::new,
            AllocateReplicaAllocationCommand::fromXContent,
            AllocateReplicaAllocationCommand.COMMAND_NAME_FIELD
        );
        registerAllocationCommand(
            AllocateEmptyPrimaryAllocationCommand::new,
            AllocateEmptyPrimaryAllocationCommand::fromXContent,
            AllocateEmptyPrimaryAllocationCommand.COMMAND_NAME_FIELD
        );
        registerAllocationCommand(
            AllocateStalePrimaryAllocationCommand::new,
            AllocateStalePrimaryAllocationCommand::fromXContent,
            AllocateStalePrimaryAllocationCommand.COMMAND_NAME_FIELD
        );
        namedWriteables.add(new NamedWriteableRegistry.Entry(Task.Status.class, ReplicationTask.Status.NAME, ReplicationTask.Status::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(Task.Status.class, RawTaskStatus.NAME, RawTaskStatus::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(Task.Status.class, ResyncTask.Status.NAME, ResyncTask.Status::new));
    }

    private final Map<String, Supplier<Transport>> transportFactories = new HashMap<>();
    private final Map<String, Supplier<HttpServerTransport>> transportHttpFactories = new HashMap<>();
    private final List<TransportInterceptor> transportInterceptors = new ArrayList<>();

    /**
     * Creates a network module that custom networking classes can be plugged into.
     * @param settings The settings for the node
     * @param transportClient True if only transport classes should be allowed to be registered, false otherwise.
     */
    public NetworkModule(
        Settings settings,
        boolean transportClient,
        List<NetworkPlugin> plugins,
        ThreadPool threadPool,
        BigArrays bigArrays,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry,
        NamedXContentRegistry xContentRegistry,
        NetworkService networkService,
        HttpServerTransport.Dispatcher dispatcher,
        BiConsumer<HttpPreRequest, ThreadContext> perRequestThreadContext,
        ClusterSettings clusterSettings
    ) {
        this.settings = settings;
        this.transportClient = transportClient;
        for (NetworkPlugin plugin : plugins) {
            Map<String, Supplier<HttpServerTransport>> httpTransportFactory = plugin.getHttpTransports(
                settings,
                threadPool,
                bigArrays,
                pageCacheRecycler,
                circuitBreakerService,
                xContentRegistry,
                networkService,
                dispatcher,
                perRequestThreadContext,
                clusterSettings
            );
            if (transportClient == false) {
                for (Map.Entry<String, Supplier<HttpServerTransport>> entry : httpTransportFactory.entrySet()) {
                    registerHttpTransport(entry.getKey(), entry.getValue());
                }
            }
            Map<String, Supplier<Transport>> transportFactory = plugin.getTransports(
                settings,
                threadPool,
                pageCacheRecycler,
                circuitBreakerService,
                namedWriteableRegistry,
                networkService
            );
            for (Map.Entry<String, Supplier<Transport>> entry : transportFactory.entrySet()) {
                registerTransport(entry.getKey(), entry.getValue());
            }
            List<TransportInterceptor> transportInterceptors = plugin.getTransportInterceptors(
                namedWriteableRegistry,
                threadPool.getThreadContext()
            );
            for (TransportInterceptor interceptor : transportInterceptors) {
                registerTransportInterceptor(interceptor);
            }
        }
    }

    public boolean isTransportClient() {
        return transportClient;
    }

    /** Adds a transport implementation that can be selected by setting {@link #TRANSPORT_TYPE_KEY}. */
    private void registerTransport(String key, Supplier<Transport> factory) {
        if (transportFactories.putIfAbsent(key, factory) != null) {
            throw new IllegalArgumentException("transport for name: " + key + " is already registered");
        }
    }

    /** Adds an http transport implementation that can be selected by setting {@link #HTTP_TYPE_KEY}. */
    // TODO: we need another name than "http transport"....so confusing with transportClient...
    private void registerHttpTransport(String key, Supplier<HttpServerTransport> factory) {
        if (transportClient) {
            throw new IllegalArgumentException("Cannot register http transport " + key + " for transport client");
        }
        if (transportHttpFactories.putIfAbsent(key, factory) != null) {
            throw new IllegalArgumentException("transport for name: " + key + " is already registered");
        }
    }

    /**
     * Register an allocation command.
     * <p>
     * This lives here instead of the more aptly named ClusterModule because the Transport client needs these to be registered.
     * </p>
     * @param reader the reader to read it from a stream
     * @param parser the parser to read it from XContent
     * @param commandName the names under which the command should be parsed. The {@link ParseField#getPreferredName()} is special because
     *        it is the name under which the command's reader is registered.
     */
    private static <T extends AllocationCommand> void registerAllocationCommand(
        Writeable.Reader<T> reader,
        CheckedFunction<XContentParser, T, IOException> parser,
        ParseField commandName
    ) {
        namedXContents.add(new NamedXContentRegistry.Entry(AllocationCommand.class, commandName, parser));
        namedWriteables.add(new NamedWriteableRegistry.Entry(AllocationCommand.class, commandName.getPreferredName(), reader));
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Collections.unmodifiableList(namedWriteables);
    }

    public static List<NamedXContentRegistry.Entry> getNamedXContents() {
        return Collections.unmodifiableList(namedXContents);
    }

    public Supplier<HttpServerTransport> getHttpServerTransportSupplier() {
        final String name;
        if (HTTP_TYPE_SETTING.exists(settings)) {
            name = HTTP_TYPE_SETTING.get(settings);
        } else {
            name = HTTP_DEFAULT_TYPE_SETTING.get(settings);
        }
        final Supplier<HttpServerTransport> factory = transportHttpFactories.get(name);
        if (factory == null) {
            throw new IllegalStateException("Unsupported http.type [" + name + "]");
        }
        return factory;
    }

    public Supplier<Transport> getTransportSupplier() {
        final String name;
        if (TRANSPORT_TYPE_SETTING.exists(settings)) {
            name = TRANSPORT_TYPE_SETTING.get(settings);
        } else {
            name = TRANSPORT_DEFAULT_TYPE_SETTING.get(settings);
        }
        final Supplier<Transport> factory = transportFactories.get(name);
        if (factory == null) {
            throw new IllegalStateException("Unsupported transport.type [" + name + "]");
        }
        return factory;
    }

    /**
     * Registers a new {@link TransportInterceptor}
     */
    private void registerTransportInterceptor(TransportInterceptor interceptor) {
        this.transportInterceptors.add(Objects.requireNonNull(interceptor, "interceptor must not be null"));
    }

    /**
     * Returns a composite {@link TransportInterceptor} containing all registered interceptors
     * @see #registerTransportInterceptor(TransportInterceptor)
     */
    public TransportInterceptor getTransportInterceptor() {
        return new CompositeTransportInterceptor(this.transportInterceptors);
    }

    static final class CompositeTransportInterceptor implements TransportInterceptor {
        final List<TransportInterceptor> transportInterceptors;

        private CompositeTransportInterceptor(List<TransportInterceptor> transportInterceptors) {
            this.transportInterceptors = new ArrayList<>(transportInterceptors);
        }

        @Override
        public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
            String action,
            String executor,
            boolean forceExecution,
            TransportRequestHandler<T> actualHandler
        ) {
            for (TransportInterceptor interceptor : this.transportInterceptors) {
                actualHandler = interceptor.interceptHandler(action, executor, forceExecution, actualHandler);
            }
            return actualHandler;
        }

        @Override
        public AsyncSender interceptSender(AsyncSender sender) {
            for (TransportInterceptor interceptor : this.transportInterceptors) {
                sender = interceptor.interceptSender(sender);
            }
            return sender;
        }
    }

}
