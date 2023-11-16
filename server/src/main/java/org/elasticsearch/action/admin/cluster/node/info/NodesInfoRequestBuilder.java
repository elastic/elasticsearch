/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

// TODO: This class's interface should match that of NodesInfoRequest
public class NodesInfoRequestBuilder extends NodesOperationRequestBuilder<NodesInfoRequest, NodesInfoResponse, NodesInfoRequestBuilder> {

    public NodesInfoRequestBuilder(ElasticsearchClient client, ActionType<NodesInfoResponse> action) {
        super(client, action, new NodesInfoRequest());
    }

    /**
     * Clears all info flags.
     */
    public NodesInfoRequestBuilder clear() {
        request.clear();
        return this;
    }

    /**
     * Sets to return all the data.
     */
    public NodesInfoRequestBuilder all() {
        request.all();
        return this;
    }

    /**
     * Should the node settings be returned.
     */
    public NodesInfoRequestBuilder setSettings(boolean settings) {
        addOrRemoveMetric(settings, NodesInfoMetrics.Metric.SETTINGS);
        return this;
    }

    /**
     * Should the node OS info be returned.
     */
    public NodesInfoRequestBuilder setOs(boolean os) {
        addOrRemoveMetric(os, NodesInfoMetrics.Metric.OS);
        return this;
    }

    /**
     * Should the node OS process be returned.
     */
    public NodesInfoRequestBuilder setProcess(boolean process) {
        addOrRemoveMetric(process, NodesInfoMetrics.Metric.PROCESS);
        return this;
    }

    /**
     * Should the node JVM info be returned.
     */
    public NodesInfoRequestBuilder setJvm(boolean jvm) {
        addOrRemoveMetric(jvm, NodesInfoMetrics.Metric.JVM);
        return this;
    }

    /**
     * Should the node thread pool info be returned.
     */
    public NodesInfoRequestBuilder setThreadPool(boolean threadPool) {
        addOrRemoveMetric(threadPool, NodesInfoMetrics.Metric.THREAD_POOL);
        return this;
    }

    /**
     * Should the node Transport info be returned.
     */
    public NodesInfoRequestBuilder setTransport(boolean transport) {
        addOrRemoveMetric(transport, NodesInfoMetrics.Metric.TRANSPORT);
        return this;
    }

    /**
     * Should the node HTTP info be returned.
     */
    public NodesInfoRequestBuilder setHttp(boolean http) {
        addOrRemoveMetric(http, NodesInfoMetrics.Metric.HTTP);
        return this;
    }

    /**
     * Should the node plugins info be returned.
     */
    public NodesInfoRequestBuilder setPlugins(boolean plugins) {
        addOrRemoveMetric(plugins, NodesInfoMetrics.Metric.PLUGINS);
        return this;
    }

    /**
     * Should the node ingest info be returned.
     */
    public NodesInfoRequestBuilder setIngest(boolean ingest) {
        addOrRemoveMetric(ingest, NodesInfoMetrics.Metric.INGEST);
        return this;
    }

    /**
     * Should the node indices info be returned.
     */
    public NodesInfoRequestBuilder setIndices(boolean indices) {
        addOrRemoveMetric(indices, NodesInfoMetrics.Metric.INDICES);
        return this;
    }

    private void addOrRemoveMetric(boolean includeMetric, NodesInfoMetrics.Metric metric) {
        if (includeMetric) {
            request.addMetric(metric.metricName());
        } else {
            request.removeMetric(metric.metricName());
        }
    }
}
