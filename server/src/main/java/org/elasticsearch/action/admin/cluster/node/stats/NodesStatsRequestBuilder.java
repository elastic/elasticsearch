/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class NodesStatsRequestBuilder extends NodesOperationRequestBuilder<
    NodesStatsRequest,
    NodesStatsResponse,
    NodesStatsRequestBuilder> {

    public NodesStatsRequestBuilder(ElasticsearchClient client) {
        super(client, TransportNodesStatsAction.TYPE, new NodesStatsRequest());
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
        addOrRemoveMetric(breaker, NodesStatsRequestParameters.Metric.BREAKER);
        return this;
    }

    public NodesStatsRequestBuilder setScript(boolean script) {
        addOrRemoveMetric(script, NodesStatsRequestParameters.Metric.SCRIPT);
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
        addOrRemoveMetric(os, NodesStatsRequestParameters.Metric.OS);
        return this;
    }

    /**
     * Should the node OS stats be returned.
     */
    public NodesStatsRequestBuilder setProcess(boolean process) {
        addOrRemoveMetric(process, NodesStatsRequestParameters.Metric.PROCESS);
        return this;
    }

    /**
     * Should the node JVM stats be returned.
     */
    public NodesStatsRequestBuilder setJvm(boolean jvm) {
        addOrRemoveMetric(jvm, NodesStatsRequestParameters.Metric.JVM);
        return this;
    }

    /**
     * Should the node thread pool stats be returned.
     */
    public NodesStatsRequestBuilder setThreadPool(boolean threadPool) {
        addOrRemoveMetric(threadPool, NodesStatsRequestParameters.Metric.THREAD_POOL);
        return this;
    }

    /**
     * Should the node file system stats be returned.
     */
    public NodesStatsRequestBuilder setFs(boolean fs) {
        addOrRemoveMetric(fs, NodesStatsRequestParameters.Metric.FS);
        return this;
    }

    /**
     * Should the node Transport stats be returned.
     */
    public NodesStatsRequestBuilder setTransport(boolean transport) {
        addOrRemoveMetric(transport, NodesStatsRequestParameters.Metric.TRANSPORT);
        return this;
    }

    /**
     * Should the node HTTP stats be returned.
     */
    public NodesStatsRequestBuilder setHttp(boolean http) {
        addOrRemoveMetric(http, NodesStatsRequestParameters.Metric.HTTP);
        return this;
    }

    /**
     * Should the discovery stats be returned.
     */
    public NodesStatsRequestBuilder setDiscovery(boolean discovery) {
        addOrRemoveMetric(discovery, NodesStatsRequestParameters.Metric.DISCOVERY);
        return this;
    }

    /**
     * Should ingest statistics be returned.
     */
    public NodesStatsRequestBuilder setIngest(boolean ingest) {
        addOrRemoveMetric(ingest, NodesStatsRequestParameters.Metric.INGEST);
        return this;
    }

    public NodesStatsRequestBuilder setAdaptiveSelection(boolean adaptiveSelection) {
        addOrRemoveMetric(adaptiveSelection, NodesStatsRequestParameters.Metric.ADAPTIVE_SELECTION);
        return this;
    }

    /**
     * Should script context cache statistics be returned
     */
    public NodesStatsRequestBuilder setScriptCache(boolean scriptCache) {
        addOrRemoveMetric(scriptCache, NodesStatsRequestParameters.Metric.SCRIPT_CACHE);
        return this;
    }

    public NodesStatsRequestBuilder setIndexingPressure(boolean indexingPressure) {
        addOrRemoveMetric(indexingPressure, NodesStatsRequestParameters.Metric.INDEXING_PRESSURE);
        return this;
    }

    public NodesStatsRequestBuilder setRepositoryStats(boolean repositoryStats) {
        addOrRemoveMetric(repositoryStats, NodesStatsRequestParameters.Metric.REPOSITORIES);
        return this;
    }

    public NodesStatsRequestBuilder setAllocationStats(boolean allocationStats) {
        addOrRemoveMetric(allocationStats, NodesStatsRequestParameters.Metric.ALLOCATIONS);
        return this;
    }

    /**
     * Helper method for adding metrics to a request
     */
    private void addOrRemoveMetric(boolean includeMetric, NodesStatsRequestParameters.Metric metric) {
        if (includeMetric) {
            request.addMetric(metric.metricName());
        } else {
            request.removeMetric(metric.metricName());
        }
    }

}
