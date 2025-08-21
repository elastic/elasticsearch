/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.node;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.ComponentVersionNumber;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.support.AggregationUsageService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class NodeService implements Closeable {
    private final Settings settings;
    private final ThreadPool threadPool;
    private final MonitorService monitorService;
    private final TransportService transportService;
    private final IndicesService indicesService;
    private final PluginsService pluginService;
    private final CircuitBreakerService circuitBreakerService;
    private final IngestService ingestService;
    private final SettingsFilter settingsFilter;
    private final ScriptService scriptService;
    private final HttpServerTransport httpServerTransport;
    private final ResponseCollectorService responseCollectorService;
    private final SearchTransportService searchTransportService;
    private final IndexingPressure indexingPressure;
    private final AggregationUsageService aggregationUsageService;
    private final Coordinator coordinator;
    private final RepositoriesService repositoriesService;
    private final Map<String, Integer> componentVersions;
    private final CompatibilityVersions compatibilityVersions;

    NodeService(
        Settings settings,
        ThreadPool threadPool,
        MonitorService monitorService,
        Coordinator coordinator,
        TransportService transportService,
        IndicesService indicesService,
        PluginsService pluginService,
        CircuitBreakerService circuitBreakerService,
        ScriptService scriptService,
        @Nullable HttpServerTransport httpServerTransport,
        IngestService ingestService,
        ClusterService clusterService,
        SettingsFilter settingsFilter,
        ResponseCollectorService responseCollectorService,
        SearchTransportService searchTransportService,
        IndexingPressure indexingPressure,
        AggregationUsageService aggregationUsageService,
        RepositoriesService repositoriesService,
        CompatibilityVersions compatibilityVersions
    ) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.monitorService = monitorService;
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.coordinator = coordinator;
        this.pluginService = pluginService;
        this.circuitBreakerService = circuitBreakerService;
        this.httpServerTransport = httpServerTransport;
        this.ingestService = ingestService;
        this.settingsFilter = settingsFilter;
        this.scriptService = scriptService;
        this.responseCollectorService = responseCollectorService;
        this.searchTransportService = searchTransportService;
        this.indexingPressure = indexingPressure;
        this.aggregationUsageService = aggregationUsageService;
        this.repositoriesService = repositoriesService;
        this.componentVersions = findComponentVersions(pluginService);
        this.compatibilityVersions = compatibilityVersions;
        clusterService.addStateApplier(ingestService);
    }

    public NodeInfo info(
        boolean settings,
        boolean os,
        boolean process,
        boolean jvm,
        boolean threadPool,
        boolean transport,
        boolean http,
        boolean remoteClusterServer,
        boolean plugin,
        boolean ingest,
        boolean aggs,
        boolean indices
    ) {
        return new NodeInfo(
            // TODO: revert to Build.current().version() when Kibana is updated
            Version.CURRENT.toString(),
            compatibilityVersions,
            IndexVersion.current(),
            componentVersions,
            Build.current(),
            transportService.getLocalNode(),
            settings ? settingsFilter.filter(this.settings) : null,
            os ? monitorService.osService().info() : null,
            process ? monitorService.processService().info() : null,
            jvm ? monitorService.jvmService().info() : null,
            threadPool ? this.threadPool.info() : null,
            transport ? transportService.info() : null,
            http ? (httpServerTransport == null ? null : httpServerTransport.info()) : null,
            remoteClusterServer ? transportService.getRemoteClusterService().info() : null,
            plugin ? (pluginService == null ? null : pluginService.info()) : null,
            ingest ? (ingestService == null ? null : ingestService.info()) : null,
            aggs ? (aggregationUsageService == null ? null : aggregationUsageService.info()) : null,
            indices ? ByteSizeValue.ofBytes(indicesService.getTotalIndexingBufferBytes()) : null
        );
    }

    private static Map<String, Integer> findComponentVersions(PluginsService pluginService) {
        var versions = pluginService.loadServiceProviders(ComponentVersionNumber.class)
            .stream()
            .collect(Collectors.toUnmodifiableMap(ComponentVersionNumber::componentId, cvn -> cvn.versionNumber().id()));

        if (Assertions.ENABLED) {
            var isSnakeCase = Pattern.compile("[a-z_]+").asMatchPredicate();
            for (String key : versions.keySet()) {
                assert isSnakeCase.test(key) : "Version component " + key + " should use snake_case";
            }
        }
        return versions;
    }

    public NodeStats stats(
        CommonStatsFlags indices,
        boolean includeShardsStats,
        boolean os,
        boolean process,
        boolean jvm,
        boolean threadPool,
        boolean fs,
        boolean transport,
        boolean http,
        boolean circuitBreaker,
        boolean script,
        boolean discoveryStats,
        boolean ingest,
        boolean adaptiveSelection,
        boolean scriptCache,
        boolean indexingPressure,
        boolean repositoriesStats
    ) {
        // for indices stats we want to include previous allocated shards stats as well (it will
        // only be applied to the sensible ones to use, like refresh/merge/flush/indexing stats)
        return new NodeStats(
            transportService.getLocalNode(),
            System.currentTimeMillis(),
            indices.anySet() ? indicesService.stats(indices, includeShardsStats) : null,
            os ? monitorService.osService().stats() : null,
            process ? monitorService.processService().stats() : null,
            jvm ? monitorService.jvmService().stats() : null,
            threadPool ? this.threadPool.stats() : null,
            fs ? monitorService.fsService().stats() : null,
            transport ? transportService.stats() : null,
            http ? (httpServerTransport == null ? null : httpServerTransport.stats()) : null,
            circuitBreaker ? circuitBreakerService.stats() : null,
            script ? scriptService.stats() : null,
            discoveryStats ? coordinator.stats() : null,
            ingest ? ingestService.stats() : null,
            adaptiveSelection ? responseCollectorService.getAdaptiveStats(searchTransportService.getPendingSearchRequests()) : null,
            scriptCache ? scriptService.cacheStats() : null,
            indexingPressure ? this.indexingPressure.stats() : null,
            repositoriesStats ? this.repositoriesService.getRepositoriesThrottlingStats() : null,
            null
        );
    }

    public IngestService getIngestService() {
        return ingestService;
    }

    public MonitorService getMonitorService() {
        return monitorService;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(indicesService);
    }

    /**
     * Wait for the node to be effectively closed.
     * @see IndicesService#awaitClose(long, TimeUnit)
     */
    public boolean awaitClose(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return indicesService.awaitClose(timeout, timeUnit);
    }

}
