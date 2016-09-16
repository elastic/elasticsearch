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

package org.elasticsearch.common.network;

import org.elasticsearch.action.support.replication.ReplicationTask;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateReplicaAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommandRegistry;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.tasks.RawTaskStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.local.LocalTransport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A module to handle registering and binding all network related classes.
 */
public final class NetworkModule {

    public static final String TRANSPORT_TYPE_KEY = "transport.type";
    public static final String HTTP_TYPE_KEY = "http.type";
    public static final String LOCAL_TRANSPORT = "local";
    public static final String HTTP_TYPE_DEFAULT_KEY = "http.type.default";
    public static final String TRANSPORT_TYPE_DEFAULT_KEY = "transport.type.default";

    public static final Setting<String> TRANSPORT_DEFAULT_TYPE_SETTING = Setting.simpleString(TRANSPORT_TYPE_DEFAULT_KEY,
            Property.NodeScope);
    public static final Setting<String> HTTP_DEFAULT_TYPE_SETTING = Setting.simpleString(HTTP_TYPE_DEFAULT_KEY, Property.NodeScope);
    public static final Setting<String> HTTP_TYPE_SETTING = Setting.simpleString(HTTP_TYPE_KEY, Property.NodeScope);
    public static final Setting<Boolean> HTTP_ENABLED = Setting.boolSetting("http.enabled", true, Property.NodeScope);
    public static final Setting<String> TRANSPORT_TYPE_SETTING = Setting.simpleString(TRANSPORT_TYPE_KEY, Property.NodeScope);

    private final Settings settings;
    private final boolean transportClient;

    private final AllocationCommandRegistry allocationCommandRegistry = new AllocationCommandRegistry();
    private final Map<String, NetworkPlugin.TransportFactory<Transport>> transportFactories = new HashMap<>();
    private final Map<String, NetworkPlugin.TransportFactory<HttpServerTransport>> transportHttpFactories = new HashMap<>();
    private final List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
    private final List<TransportInterceptor> transportIntercetors = new ArrayList<>();

    /**
     * Creates a network module that custom networking classes can be plugged into.
     * @param settings The settings for the node
     * @param transportClient True if only transport classes should be allowed to be registered, false otherwise.
     */
    public NetworkModule(Settings settings, boolean transportClient) {
        this.settings = settings;
        this.transportClient = transportClient;
        registerTransport(new NetworkPlugin.TransportFactory<Transport>(LOCAL_TRANSPORT) {

            @Override
            public Transport createTransport(Settings settings, ThreadPool threadPool, BigArrays bigArrays,
                                             CircuitBreakerService circuitBreakerService, NamedWriteableRegistry namedWriteableRegistry,
                                             NetworkService networkService) {
                return new LocalTransport(settings, threadPool, namedWriteableRegistry, circuitBreakerService);
            }
        });
        namedWriteables.add(new NamedWriteableRegistry.Entry(Task.Status.class, ReplicationTask.Status.NAME, ReplicationTask.Status::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(Task.Status.class, RawTaskStatus.NAME, RawTaskStatus::new));
        registerBuiltinAllocationCommands();
    }

    public void processPlugins(List<NetworkPlugin> plugins) {
        for (NetworkPlugin plugin : plugins) {
            if (transportClient == false && HTTP_ENABLED.get(settings)) {
                List<NetworkPlugin.TransportFactory<HttpServerTransport>> httpTransportFactory = plugin.getHttpTransportFactory();
                for (NetworkPlugin.TransportFactory<HttpServerTransport> factory : httpTransportFactory) {
                    registerHttpTransport(factory);
                }
            }
            List<NetworkPlugin.TransportFactory<Transport>> transportFactory = plugin.getTransportFactory();
            for (NetworkPlugin.TransportFactory<Transport> factory : transportFactory) {
                registerTransport(factory);
            }
            List<TransportInterceptor> transportInterceptors = plugin.getTransportInterceptors();
            for (TransportInterceptor interceptor : transportInterceptors) {
                registerTransportInterceptor(interceptor);
            }
        }
    }

    public boolean isTransportClient() {
        return transportClient;
    }

    /** Adds a transport implementation that can be selected by setting {@link #TRANSPORT_TYPE_KEY}. */
    public void registerTransport(NetworkPlugin.TransportFactory<Transport> factory) {
        if (transportFactories.putIfAbsent(factory.getType(), factory) != null) {
            throw new IllegalArgumentException("transport for name: " + factory.getType() + " is already registered");
        }
    }

    /** Adds an http transport implementation that can be selected by setting {@link #HTTP_TYPE_KEY}. */
    // TODO: we need another name than "http transport"....so confusing with transportClient...
    public void registerHttpTransport(NetworkPlugin.TransportFactory<HttpServerTransport> factory) {
        if (transportClient) {
            throw new IllegalArgumentException("Cannot register http transport " + factory.getType() + " for transport client");
        }
        if (transportHttpFactories.putIfAbsent(factory.getType(), factory) != null) {
            throw new IllegalArgumentException("transport for name: " + factory.getType() + " is already registered");
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
    private <T extends AllocationCommand> void registerAllocationCommand(Writeable.Reader<T> reader, AllocationCommand.Parser<T> parser,
            ParseField commandName) {
        allocationCommandRegistry.register(parser, commandName);
        namedWriteables.add(new Entry(AllocationCommand.class, commandName.getPreferredName(), reader));
    }

    /**
     * The registry of allocation command parsers.
     */
    public AllocationCommandRegistry getAllocationCommandRegistry() {
        return allocationCommandRegistry;
    }

    public List<Entry> getNamedWriteables() {
        return namedWriteables;
    }

    public NetworkPlugin.TransportFactory<HttpServerTransport> getHttpServerTransportFactory() {
        final String name;
        if (HTTP_TYPE_SETTING.exists(settings)) {
            name = HTTP_TYPE_SETTING.get(settings);
        } else {
            name = HTTP_DEFAULT_TYPE_SETTING.get(settings);
        }
        final NetworkPlugin.TransportFactory<HttpServerTransport> factory = transportHttpFactories.get(name);
        if (factory == null) {
            throw new IllegalStateException("No http.type: " + HTTP_TYPE_SETTING.get(settings) + " configured");
        }
        return factory;
    }

    public boolean isHttpEnabled() {
        return transportClient == false && HTTP_ENABLED.get(settings);
    }

    public NetworkPlugin.TransportFactory<Transport> getTransportFactory() {
        final String name;
        if (TRANSPORT_TYPE_SETTING.exists(settings)) {
            name = TRANSPORT_TYPE_SETTING.get(settings);
        } else {
            name = TRANSPORT_DEFAULT_TYPE_SETTING.get(settings);
        }
        final NetworkPlugin.TransportFactory<Transport> factory = transportFactories.get(name);
        if (factory == null) {
            throw new IllegalStateException("No transport.type: " + name + " configured");
        }
        return factory;
    }

    private void registerBuiltinAllocationCommands() {
        registerAllocationCommand(CancelAllocationCommand::new, CancelAllocationCommand::fromXContent,
                CancelAllocationCommand.COMMAND_NAME_FIELD);
        registerAllocationCommand(MoveAllocationCommand::new, MoveAllocationCommand::fromXContent,
                MoveAllocationCommand.COMMAND_NAME_FIELD);
        registerAllocationCommand(AllocateReplicaAllocationCommand::new, AllocateReplicaAllocationCommand::fromXContent,
                AllocateReplicaAllocationCommand.COMMAND_NAME_FIELD);
        registerAllocationCommand(AllocateEmptyPrimaryAllocationCommand::new, AllocateEmptyPrimaryAllocationCommand::fromXContent,
                AllocateEmptyPrimaryAllocationCommand.COMMAND_NAME_FIELD);
        registerAllocationCommand(AllocateStalePrimaryAllocationCommand::new, AllocateStalePrimaryAllocationCommand::fromXContent,
                AllocateStalePrimaryAllocationCommand.COMMAND_NAME_FIELD);

    }

    /**
     * Registers a new {@link TransportInterceptor}
     */
    public void registerTransportInterceptor(TransportInterceptor interceptor) {
        this.transportIntercetors.add(Objects.requireNonNull(interceptor, "interceptor must not be null"));
    }

    /**
     * Returns a composite {@link TransportInterceptor} containing all registered interceptors
     * @see #registerTransportInterceptor(TransportInterceptor)
     */
    public TransportInterceptor getTransportInterceptor() {
        return new CompositeTransportInterceptor(this.transportIntercetors);
    }

    static final class CompositeTransportInterceptor implements TransportInterceptor {
        final List<TransportInterceptor> transportInterceptors;

        private CompositeTransportInterceptor(List<TransportInterceptor> transportInterceptors) {
            this.transportInterceptors = new ArrayList<>(transportInterceptors);
        }

        @Override
        public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action, TransportRequestHandler<T> actualHandler) {
            for (TransportInterceptor interceptor : this.transportInterceptors) {
                actualHandler = interceptor.interceptHandler(action, actualHandler);
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
