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

import org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.local.LocalTransport;
import org.elasticsearch.transport.netty.NettyTransport;

import static org.elasticsearch.common.Preconditions.checkNotNull;

/**
 *
 */
public class TransportModule extends AbstractModule {

    public static final String TRANSPORT_TYPE_KEY = "transport.type";
    public static final String TRANSPORT_SERVICE_TYPE_KEY = "transport.service.type";

    private final ESLogger logger;
    private final Settings settings;

    private Class<? extends TransportService> configuredTransportService;
    private Class<? extends Transport> configuredTransport;
    private String configuredTransportServiceSource;
    private String configuredTransportSource;

    public TransportModule(Settings settings) {
        this.settings = settings;
        this.logger = Loggers.getLogger(getClass(), settings);
    }

    @Override
    protected void configure() {
        if (configuredTransportService != null) {
            logger.info("Using [{}] as transport service, overridden by [{}]", configuredTransportService.getName(), configuredTransportServiceSource);
            bind(TransportService.class).to(configuredTransportService).asEagerSingleton();
        } else {
            Class<? extends TransportService> defaultTransportService = TransportService.class;
            Class<? extends TransportService> transportService = settings.getAsClass(TRANSPORT_SERVICE_TYPE_KEY, defaultTransportService, "org.elasticsearch.transport.", "TransportService");
            if (!TransportService.class.equals(transportService)) {
                bind(TransportService.class).to(transportService).asEagerSingleton();
            } else {
                bind(TransportService.class).asEagerSingleton();
            }
        }

        if (configuredTransport != null) {
            logger.info("Using [{}] as transport, overridden by [{}]", configuredTransport.getName(), configuredTransportSource);
            bind(Transport.class).to(configuredTransport).asEagerSingleton();
        } else {
            Class<? extends Transport> defaultTransport = DiscoveryNode.localNode(settings) ? LocalTransport.class : NettyTransport.class;
            Class<? extends Transport> transport = settings.getAsClass(TRANSPORT_TYPE_KEY, defaultTransport, "org.elasticsearch.transport.", "Transport");
            bind(Transport.class).to(transport).asEagerSingleton();
        }
    }

    public void setTransportService(Class<? extends TransportService> transportService, String source) {
        checkNotNull(transportService, "Configured transport service may not be null");
        checkNotNull(source, "Plugin, that changes transport service may not be null");
        this.configuredTransportService = transportService;
        this.configuredTransportServiceSource = source;
    }

    public void setTransport(Class<? extends Transport> transport, String source) {
        checkNotNull(transport, "Configured transport may not be null");
        checkNotNull(source, "Plugin, that changes transport may not be null");
        this.configuredTransport = transport;
        this.configuredTransportSource = source;
    }
}