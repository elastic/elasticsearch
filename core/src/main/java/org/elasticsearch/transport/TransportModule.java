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

package org.elasticsearch.transport;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.local.LocalTransport;
import org.elasticsearch.transport.netty.NettyTransport;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class TransportModule extends AbstractModule {

    public static final String TRANSPORT_TYPE_KEY = "transport.type";
    public static final String TRANSPORT_SERVICE_TYPE_KEY = "transport.service.type";

    public static final String LOCAL_TRANSPORT = "local";
    public static final String NETTY_TRANSPORT = "netty";

    private final ESLogger logger;
    private final Settings settings;

    private final Map<String, Class<? extends TransportService>> transportServices = new HashMap<>();
    private final Map<String, Class<? extends Transport>> transports = new HashMap<>();
    private Class<? extends TransportService> configuredTransportService;
    private Class<? extends Transport> configuredTransport;
    private String configuredTransportServiceSource;
    private String configuredTransportSource;

    public TransportModule(Settings settings) {
        this.settings = settings;
        this.logger = Loggers.getLogger(getClass(), settings);
        addTransport(LOCAL_TRANSPORT, LocalTransport.class);
        addTransport(NETTY_TRANSPORT, NettyTransport.class);
    }

    public void addTransportService(String name, Class<? extends TransportService> clazz) {
        Class<? extends TransportService> oldClazz = transportServices.put(name, clazz);
        if (oldClazz != null) {
            throw new IllegalArgumentException("Cannot register TransportService [" + name + "] to " + clazz.getName() + ", already registered to " + oldClazz.getName());
        }
    }

    public void addTransport(String name, Class<? extends Transport> clazz) {
        Class<? extends Transport> oldClazz = transports.put(name, clazz);
        if (oldClazz != null) {
            throw new IllegalArgumentException("Cannot register Transport [" + name + "] to " + clazz.getName() + ", already registered to " + oldClazz.getName());
        }
    }

    @Override
    protected void configure() {
        if (configuredTransportService != null) {
            logger.info("Using [{}] as transport service, overridden by [{}]", configuredTransportService.getName(), configuredTransportServiceSource);
            bind(TransportService.class).to(configuredTransportService).asEagerSingleton();
        } else {
            String typeName = settings.get(TRANSPORT_SERVICE_TYPE_KEY);
            if (typeName == null) {
                bind(TransportService.class).asEagerSingleton();
            } else {
                if (transportServices.containsKey(typeName) == false) {
                    throw new IllegalArgumentException("Unknown TransportService type [" + typeName + "], known types are: " + transportServices.keySet());
                }
                bind(TransportService.class).to(transportServices.get(typeName)).asEagerSingleton();
            }
        }

        bind(NamedWriteableRegistry.class).asEagerSingleton();
        if (configuredTransport != null) {
            logger.info("Using [{}] as transport, overridden by [{}]", configuredTransport.getName(), configuredTransportSource);
            bind(Transport.class).to(configuredTransport).asEagerSingleton();
        } else {
            String defaultType = DiscoveryNode.localNode(settings) ? LOCAL_TRANSPORT : NETTY_TRANSPORT;
            String typeName = settings.get(TRANSPORT_TYPE_KEY, defaultType);
            Class<? extends Transport> clazz = transports.get(typeName);
            if (clazz == null) {
                throw new IllegalArgumentException("Unknown Transport [" + typeName + "]");
            }
            bind(Transport.class).to(clazz).asEagerSingleton();
        }
    }

    public void setTransportService(Class<? extends TransportService> transportService, String source) {
        Objects.requireNonNull(transportService, "Configured transport service may not be null");
        Objects.requireNonNull(source, "Plugin, that changes transport service may not be null");
        this.configuredTransportService = transportService;
        this.configuredTransportServiceSource = source;
    }

    public void setTransport(Class<? extends Transport> transport, String source) {
        Objects.requireNonNull(transport, "Configured transport may not be null");
        Objects.requireNonNull(source, "Plugin, that changes transport may not be null");
        this.configuredTransport = transport;
        this.configuredTransportSource = source;
    }
}