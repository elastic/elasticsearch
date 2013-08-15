/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.node.service;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.http.HttpServer;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.net.InetAddress;

/**
 */
public class NodeService extends AbstractComponent {

    private final ThreadPool threadPool;

    private final MonitorService monitorService;

    private final TransportService transportService;

    private final IndicesService indicesService;

    private final PluginsService pluginService;

    @Nullable
    private HttpServer httpServer;

    private volatile ImmutableMap<String, String> serviceAttributes = ImmutableMap.of();

    @Nullable
    private String hostname;

    private final Version version;

    private final Discovery disovery;

    @Inject
    public NodeService(Settings settings, ThreadPool threadPool, MonitorService monitorService, Discovery discovery,
                       TransportService transportService, IndicesService indicesService,
                       PluginsService pluginService, Version version) {
        super(settings);
        this.threadPool = threadPool;
        this.monitorService = monitorService;
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.disovery = discovery;
        discovery.setNodeService(this);
        InetAddress address = NetworkUtils.getLocalAddress();
        if (address != null) {
            this.hostname = address.getHostName();
        }
        this.version = version;
        this.pluginService = pluginService;
    }

    public void setHttpServer(@Nullable HttpServer httpServer) {
        this.httpServer = httpServer;
    }

    @Deprecated
    public void putNodeAttribute(String key, String value) {
        putAttribute(key, value);
    }

    @Deprecated
    public void removeNodeAttribute(String key) {
        removeAttribute(key);
    }

    public synchronized void putAttribute(String key, String value) {
        serviceAttributes = new MapBuilder<String, String>(serviceAttributes).put(key, value).immutableMap();
    }

    public synchronized void removeAttribute(String key) {
        serviceAttributes = new MapBuilder<String, String>(serviceAttributes).remove(key).immutableMap();
    }

    /**
     * Attributes different services in the node can add to be reported as part of the node info (for example).
     */
    public ImmutableMap<String, String> attributes() {
        return this.serviceAttributes;
    }

    public NodeInfo info() {
        return new NodeInfo(hostname, version, disovery.localNode(), serviceAttributes,
                settings,
                monitorService.osService().info(),
                monitorService.processService().info(),
                monitorService.jvmService().info(),
                threadPool.info(),
                monitorService.networkService().info(),
                transportService.info(),
                httpServer == null ? null : httpServer.info(),
                pluginService == null ? null : pluginService.info()
        );
    }

    public NodeInfo info(boolean settings, boolean os, boolean process, boolean jvm, boolean threadPool,
                         boolean network, boolean transport, boolean http, boolean plugin) {
        return new NodeInfo(hostname, version, disovery.localNode(), serviceAttributes,
                settings ? this.settings : null,
                os ? monitorService.osService().info() : null,
                process ? monitorService.processService().info() : null,
                jvm ? monitorService.jvmService().info() : null,
                threadPool ? this.threadPool.info() : null,
                network ? monitorService.networkService().info() : null,
                transport ? transportService.info() : null,
                http ? (httpServer == null ? null : httpServer.info()) : null,
                plugin ? (pluginService == null ? null : pluginService.info()) : null
        );
    }

    public NodeStats stats() {
        // for indices stats we want to include previous allocated shards stats as well (it will
        // only be applied to the sensible ones to use, like refresh/merge/flush/indexing stats)
        return new NodeStats(disovery.localNode(), System.currentTimeMillis(), hostname,
                indicesService.stats(true),
                monitorService.osService().stats(),
                monitorService.processService().stats(),
                monitorService.jvmService().stats(),
                threadPool.stats(),
                monitorService.networkService().stats(),
                monitorService.fsService().stats(),
                transportService.stats(),
                httpServer == null ? null : httpServer.stats()
        );
    }

    public NodeStats stats(CommonStatsFlags indices, boolean os, boolean process, boolean jvm, boolean threadPool, boolean network, boolean fs, boolean transport, boolean http) {
        // for indices stats we want to include previous allocated shards stats as well (it will
        // only be applied to the sensible ones to use, like refresh/merge/flush/indexing stats)
        return new NodeStats(disovery.localNode(), System.currentTimeMillis(), hostname,
                indices.anySet() ? indicesService.stats(true, indices) : null,
                os ? monitorService.osService().stats() : null,
                process ? monitorService.processService().stats() : null,
                jvm ? monitorService.jvmService().stats() : null,
                threadPool ? this.threadPool.stats() : null,
                network ? monitorService.networkService().stats() : null,
                fs ? monitorService.fsService().stats() : null,
                transport ? transportService.stats() : null,
                http ? (httpServer == null ? null : httpServer.stats()) : null
        );
    }
}
