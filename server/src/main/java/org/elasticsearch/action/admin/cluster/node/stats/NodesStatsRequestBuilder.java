/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class NodesStatsRequestBuilder extends NodesOperationRequestBuilder<
    NodesStatsRequest,
    NodesStatsResponse,
    NodesStatsRequestBuilder> {

    public NodesStatsRequestBuilder(ElasticsearchClient client, ActionType<NodesStatsResponse> action) {
        super(client, action, new NodesStatsRequest());
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
        addOrRemoveMetric(breaker, NodesStatsMetrics.Metric.BREAKER);
        return this;
    }

    public NodesStatsRequestBuilder setScript(boolean script) {
        addOrRemoveMetric(script, NodesStatsMetrics.Metric.SCRIPT);
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
        addOrRemoveMetric(os, NodesStatsMetrics.Metric.OS);
        return this;
    }

    /**
     * Should the node OS stats be returned.
     */
    public NodesStatsRequestBuilder setProcess(boolean process) {
        addOrRemoveMetric(process, NodesStatsMetrics.Metric.PROCESS);
        return this;
    }

    /**
     * Should the node JVM stats be returned.
     */
    public NodesStatsRequestBuilder setJvm(boolean jvm) {
        addOrRemoveMetric(jvm, NodesStatsMetrics.Metric.JVM);
        return this;
    }

    /**
     * Should the node thread pool stats be returned.
     */
    public NodesStatsRequestBuilder setThreadPool(boolean threadPool) {
        addOrRemoveMetric(threadPool, NodesStatsMetrics.Metric.THREAD_POOL);
        return this;
    }

    /**
     * Should the node file system stats be returned.
     */
    public NodesStatsRequestBuilder setFs(boolean fs) {
        addOrRemoveMetric(fs, NodesStatsMetrics.Metric.FS);
        return this;
    }

    /**
     * Should the node Transport stats be returned.
     */
    public NodesStatsRequestBuilder setTransport(boolean transport) {
        addOrRemoveMetric(transport, NodesStatsMetrics.Metric.TRANSPORT);
        return this;
    }

    /**
     * Should the node HTTP stats be returned.
     */
    public NodesStatsRequestBuilder setHttp(boolean http) {
        addOrRemoveMetric(http, NodesStatsMetrics.Metric.HTTP);
        return this;
    }

    /**
     * Should the discovery stats be returned.
     */
    public NodesStatsRequestBuilder setDiscovery(boolean discovery) {
        addOrRemoveMetric(discovery, NodesStatsMetrics.Metric.DISCOVERY);
        return this;
    }

    /**
     * Should ingest statistics be returned.
     */
    public NodesStatsRequestBuilder setIngest(boolean ingest) {
        addOrRemoveMetric(ingest, NodesStatsMetrics.Metric.INGEST);
        return this;
    }

    public NodesStatsRequestBuilder setAdaptiveSelection(boolean adaptiveSelection) {
        addOrRemoveMetric(adaptiveSelection, NodesStatsMetrics.Metric.ADAPTIVE_SELECTION);
        return this;
    }

    /**
     * Should script context cache statistics be returned
     */
    public NodesStatsRequestBuilder setScriptCache(boolean scriptCache) {
        addOrRemoveMetric(scriptCache, NodesStatsMetrics.Metric.SCRIPT_CACHE);
        return this;
    }

    public NodesStatsRequestBuilder setIndexingPressure(boolean indexingPressure) {
        addOrRemoveMetric(indexingPressure, NodesStatsMetrics.Metric.INDEXING_PRESSURE);
        return this;
    }

    public NodesStatsRequestBuilder setRepositoryStats(boolean repositoryStats) {
        addOrRemoveMetric(repositoryStats, NodesStatsMetrics.Metric.REPOSITORIES);
        return this;
    }

    /**
     * Helper method for adding metrics to a request
     */
    private void addOrRemoveMetric(boolean includeMetric, NodesStatsMetrics.Metric metric) {
        if (includeMetric) {
            request.addMetric(metric.metricName());
        } else {
            request.removeMetric(metric.metricName());
        }
    }

}
