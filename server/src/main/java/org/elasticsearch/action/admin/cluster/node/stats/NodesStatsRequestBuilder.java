/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class NodesStatsRequestBuilder extends NodesOperationRequestBuilder<
    NodesStatsRequest,
    NodesStatsResponse,
    NodesStatsRequestBuilder> {

    public NodesStatsRequestBuilder(ElasticsearchClient client, String[] nodeIds) {
        super(client, TransportNodesStatsAction.TYPE, new NodesStatsRequest(nodeIds));
    }

    /**
     * Sets all the request flags.
     */
    public NodesStatsRequestBuilder all() {
        request.all();
        return this;
    }

    /**
     * Clears all stats flags.
     */
    public NodesStatsRequestBuilder clear() {
        request.clear();
        return this;
    }

    /**
     * Should the node indices stats be returned.
     */
    public NodesStatsRequestBuilder setIndices(boolean indices) {
        request.indices(indices);
        return this;
    }

    public NodesStatsRequestBuilder setBreaker(boolean breaker) {
        addOrRemoveMetric(breaker, Metric.BREAKER);
        return this;
    }

    public NodesStatsRequestBuilder setScript(boolean script) {
        addOrRemoveMetric(script, Metric.SCRIPT);
        return this;
    }

    /**
     * Should the node indices stats be returned.
     */
    public NodesStatsRequestBuilder setIndices(CommonStatsFlags indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Should the node OS stats be returned.
     */
    public NodesStatsRequestBuilder setOs(boolean os) {
        addOrRemoveMetric(os, Metric.OS);
        return this;
    }

    /**
     * Should the node OS stats be returned.
     */
    public NodesStatsRequestBuilder setProcess(boolean process) {
        addOrRemoveMetric(process, Metric.PROCESS);
        return this;
    }

    /**
     * Should the node JVM stats be returned.
     */
    public NodesStatsRequestBuilder setJvm(boolean jvm) {
        addOrRemoveMetric(jvm, Metric.JVM);
        return this;
    }

    /**
     * Should the node thread pool stats be returned.
     */
    public NodesStatsRequestBuilder setThreadPool(boolean threadPool) {
        addOrRemoveMetric(threadPool, Metric.THREAD_POOL);
        return this;
    }

    /**
     * Should the node file system stats be returned.
     */
    public NodesStatsRequestBuilder setFs(boolean fs) {
        addOrRemoveMetric(fs, Metric.FS);
        return this;
    }

    /**
     * Should the node Transport stats be returned.
     */
    public NodesStatsRequestBuilder setTransport(boolean transport) {
        addOrRemoveMetric(transport, Metric.TRANSPORT);
        return this;
    }

    /**
     * Should the node HTTP stats be returned.
     */
    public NodesStatsRequestBuilder setHttp(boolean http) {
        addOrRemoveMetric(http, Metric.HTTP);
        return this;
    }

    /**
     * Should the discovery stats be returned.
     */
    public NodesStatsRequestBuilder setDiscovery(boolean discovery) {
        addOrRemoveMetric(discovery, Metric.DISCOVERY);
        return this;
    }

    /**
     * Should ingest statistics be returned.
     */
    public NodesStatsRequestBuilder setIngest(boolean ingest) {
        addOrRemoveMetric(ingest, Metric.INGEST);
        return this;
    }

    public NodesStatsRequestBuilder setAdaptiveSelection(boolean adaptiveSelection) {
        addOrRemoveMetric(adaptiveSelection, Metric.ADAPTIVE_SELECTION);
        return this;
    }

    /**
     * Should script context cache statistics be returned
     */
    public NodesStatsRequestBuilder setScriptCache(boolean scriptCache) {
        addOrRemoveMetric(scriptCache, Metric.SCRIPT_CACHE);
        return this;
    }

    public NodesStatsRequestBuilder setIndexingPressure(boolean indexingPressure) {
        addOrRemoveMetric(indexingPressure, Metric.INDEXING_PRESSURE);
        return this;
    }

    public NodesStatsRequestBuilder setRepositoryStats(boolean repositoryStats) {
        addOrRemoveMetric(repositoryStats, Metric.REPOSITORIES);
        return this;
    }

    public NodesStatsRequestBuilder setAllocationStats(boolean allocationStats) {
        addOrRemoveMetric(allocationStats, Metric.ALLOCATIONS);
        return this;
    }

    /**
     * Helper method for adding metrics to a request
     */
    private void addOrRemoveMetric(boolean includeMetric, Metric metric) {
        if (includeMetric) {
            request.addMetric(metric);
        } else {
            request.removeMetric(metric);
        }
    }

}
