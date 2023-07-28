/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

// TODO: This class's interface should match that of NodesInfoRequest
public class NodesInfoRequestBuilder extends NodesOperationRequestBuilder<NodesInfoRequest, NodesInfoResponse, NodesInfoRequestBuilder> {

    public NodesInfoRequestBuilder(ElasticsearchClient client, NodesInfoAction action) {
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
        addOrRemoveMetric(settings, NodesInfoRequest.Metric.SETTINGS);
        return this;
    }

    /**
     * Should the node Transport info be returned.
     */
    public NodesInfoRequestBuilder setTransport(boolean transport) {
        addOrRemoveMetric(transport, NodesInfoRequest.Metric.TRANSPORT);
        return this;
    }

    /**
     * Should the node HTTP info be returned.
     */
    public NodesInfoRequestBuilder setHttp(boolean http) {
        addOrRemoveMetric(http, NodesInfoRequest.Metric.HTTP);
        return this;
    }

    /**
     * Should the node plugins info be returned.
     */
    public NodesInfoRequestBuilder setPlugins(boolean plugins) {
        addOrRemoveMetric(plugins, NodesInfoRequest.Metric.PLUGINS);
        return this;
    }

    /**
     * Should the node indices info be returned.
     */
    public NodesInfoRequestBuilder setIndices(boolean indices) {
        addOrRemoveMetric(indices, NodesInfoRequest.Metric.INDICES);
        return this;
    }

    private void addOrRemoveMetric(boolean includeMetric, NodesInfoRequest.Metric metric) {
        if (includeMetric) {
            request.addMetric(metric.metricName());
        } else {
            request.removeMetric(metric.metricName());
        }
    }
}
