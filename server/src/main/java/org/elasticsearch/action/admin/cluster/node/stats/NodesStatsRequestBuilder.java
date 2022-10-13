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

    public NodesStatsRequestBuilder(ElasticsearchClient client, NodesStatsAction action) {
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
        addOrRemoveMetric(breaker, NodesStatsRequest.Metric.BREAKER);
        return this;
    }

    public NodesStatsRequestBuilder setScript(boolean script) {
        addOrRemoveMetric(script, NodesStatsRequest.Metric.SCRIPT);
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
        addOrRemoveMetric(os, NodesStatsRequest.Metric.OS);
        return this;
    }

    /**
     * Should the node OS stats be returned.
     */
    public NodesStatsRequestBuilder setProcess(boolean process) {
        addOrRemoveMetric(process, NodesStatsRequest.Metric.PROCESS);
        return this;
    }

    /**
     * Should the node JVM stats be returned.
     */
    public NodesStatsRequestBuilder setJvm(boolean jvm) {
        addOrRemoveMetric(jvm, NodesStatsRequest.Metric.JVM);
        return this;
    }

    /**
     * Should the node thread pool stats be returned.
     */
    public NodesStatsRequestBuilder setThreadPool(boolean threadPool) {
        addOrRemoveMetric(threadPool, NodesStatsRequest.Metric.THREAD_POOL);
        return this;
    }

    /**
     * Should the node file system stats be returned.
     */
    public NodesStatsRequestBuilder setFs(boolean fs) {
        addOrRemoveMetric(fs, NodesStatsRequest.Metric.FS);
        return this;
    }

    /**
     * Should the node Transport stats be returned.
     */
    public NodesStatsRequestBuilder setTransport(boolean transport) {
        addOrRemoveMetric(transport, NodesStatsRequest.Metric.TRANSPORT);
        return this;
    }

    /**
     * Should the node HTTP stats be returned.
     */
    public NodesStatsRequestBuilder setHttp(boolean http) {
        addOrRemoveMetric(http, NodesStatsRequest.Metric.HTTP);
        return this;
    }

    /**
     * Should the discovery stats be returned.
     */
    public NodesStatsRequestBuilder setDiscovery(boolean discovery) {
        addOrRemoveMetric(discovery, NodesStatsRequest.Metric.DISCOVERY);
        return this;
    }

    /**
     * Should ingest statistics be returned.
     */
    public NodesStatsRequestBuilder setIngest(boolean ingest) {
        addOrRemoveMetric(ingest, NodesStatsRequest.Metric.INGEST);
        return this;
    }

    public NodesStatsRequestBuilder setAdaptiveSelection(boolean adaptiveSelection) {
        addOrRemoveMetric(adaptiveSelection, NodesStatsRequest.Metric.ADAPTIVE_SELECTION);
        return this;
    }

    /**
     * Should script context cache statistics be returned
     */
    public NodesStatsRequestBuilder setScriptCache(boolean scriptCache) {
        addOrRemoveMetric(scriptCache, NodesStatsRequest.Metric.SCRIPT_CACHE);
        return this;
    }

    public NodesStatsRequestBuilder setIndexingPressure(boolean indexingPressure) {
        addOrRemoveMetric(indexingPressure, NodesStatsRequest.Metric.INDEXING_PRESSURE);
        return this;
    }

    /**
     * Helper method for adding metrics to a request
     */
    private void addOrRemoveMetric(boolean includeMetric, NodesStatsRequest.Metric metric) {
        if (includeMetric) {
            request.addMetric(metric.metricName());
        } else {
            request.removeMetric(metric.metricName());
        }
    }

}
