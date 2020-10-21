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

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class NodesStatsRequestBuilder
        extends NodesOperationRequestBuilder<NodesStatsRequest, NodesStatsResponse, NodesStatsRequestBuilder> {

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
