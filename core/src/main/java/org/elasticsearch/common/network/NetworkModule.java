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

import org.elasticsearch.client.support.Headers;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.client.transport.support.TransportProxyClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ExtensionPoint;
import org.elasticsearch.http.HttpServer;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.netty.NettyHttpServerTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.local.LocalTransport;
import org.elasticsearch.transport.netty.NettyTransport;

/**
 * A module to handle registering and binding all network related classes.
 */
public class NetworkModule extends AbstractModule {

    public static final String TRANSPORT_TYPE_KEY = "transport.type";
    public static final String TRANSPORT_SERVICE_TYPE_KEY = "transport.service.type";

    public static final String LOCAL_TRANSPORT = "local";
    public static final String NETTY_TRANSPORT = "netty";

    public static final String HTTP_TYPE_KEY = "http.type";
    public static final String HTTP_ENABLED = "http.enabled";

    private final NetworkService networkService;
    private final Settings settings;
    private final boolean transportClient;

    private final ExtensionPoint.SelectedType<TransportService> transportServiceTypes = new ExtensionPoint.SelectedType<>("transport_service", TransportService.class);
    private final ExtensionPoint.SelectedType<Transport> transportTypes = new ExtensionPoint.SelectedType<>("transport", Transport.class);
    private final ExtensionPoint.SelectedType<HttpServerTransport> httpTransportTypes = new ExtensionPoint.SelectedType<>("http_transport", HttpServerTransport.class);

    /**
     * Creates a network module that custom networking classes can be plugged into.
     *
     * @param networkService A constructed network service object to bind.
     * @param settings The settings for the node
     * @param transportClient True if only transport classes should be allowed to be registered, false otherwise.
     */
    public NetworkModule(NetworkService networkService, Settings settings, boolean transportClient) {
        this.networkService = networkService;
        this.settings = settings;
        this.transportClient = transportClient;
        registerTransportService(NETTY_TRANSPORT, TransportService.class);
        registerTransport(LOCAL_TRANSPORT, LocalTransport.class);
        registerTransport(NETTY_TRANSPORT, NettyTransport.class);

        if (transportClient == false) {
            registerHttpTransport(NETTY_TRANSPORT, NettyHttpServerTransport.class);
        }
    }

    /** Adds a transport service implementation that can be selected by setting {@link #TRANSPORT_SERVICE_TYPE_KEY}. */
    public void registerTransportService(String name, Class<? extends TransportService> clazz) {
        transportServiceTypes.registerExtension(name, clazz);
    }

    /** Adds a transport implementation that can be selected by setting {@link #TRANSPORT_TYPE_KEY}. */
    public void registerTransport(String name, Class<? extends Transport> clazz) {
        transportTypes.registerExtension(name, clazz);
    }

    /** Adds an http transport implementation that can be selected by setting {@link #HTTP_TYPE_KEY}. */
    // TODO: we need another name than "http transport"....so confusing with transportClient...
    public void registerHttpTransport(String name, Class<? extends HttpServerTransport> clazz) {
        if (transportClient) {
            throw new IllegalArgumentException("Cannot register http transport " + clazz.getName() + " for transport client");
        }
        httpTransportTypes.registerExtension(name, clazz);
    }

    @Override
    protected void configure() {
        bind(NetworkService.class).toInstance(networkService);
        bind(NamedWriteableRegistry.class).asEagerSingleton();

        transportServiceTypes.bindType(binder(), settings, TRANSPORT_SERVICE_TYPE_KEY, NETTY_TRANSPORT);
        String defaultTransport = DiscoveryNode.localNode(settings) ? LOCAL_TRANSPORT : NETTY_TRANSPORT;
        transportTypes.bindType(binder(), settings, TRANSPORT_TYPE_KEY, defaultTransport);

        if (transportClient) {
            bind(Headers.class).asEagerSingleton();
            bind(TransportProxyClient.class).asEagerSingleton();
            bind(TransportClientNodesService.class).asEagerSingleton();
        } else {
            if (settings.getAsBoolean(HTTP_ENABLED, true)) {
                bind(HttpServer.class).asEagerSingleton();
                httpTransportTypes.bindType(binder(), settings, HTTP_TYPE_KEY, NETTY_TRANSPORT);
            }
        }
    }
}
