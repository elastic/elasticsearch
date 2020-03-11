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

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A request to get node (cluster) level information.
 */
public class NodesInfoRequest extends BaseNodesRequest<NodesInfoRequest> {

    private Set<String> requestedMetrics = Metrics.allMetrics();

    /**
     * Create a new NodeInfoRequest from a {@link StreamInput} object.
     *
     * @param in A stream input object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public NodesInfoRequest(StreamInput in) throws IOException {
        super(in);
        requestedMetrics.clear();
        if (in.getVersion().before(Version.V_7_7_0)){
            // prior to version 8.x, a NodesInfoRequest was serialized as a list
            // of booleans in a fixed order
            addOrRemoveMetric(in.readBoolean(), Metrics.SETTINGS.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.OS.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.PROCESS.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.JVM.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.THREAD_POOL.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.TRANSPORT.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.HTTP.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.PLUGINS.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.INGEST.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.INDICES.metricName());
        } else {
            requestedMetrics.addAll(Arrays.asList(in.readStringArray()));
        }
    }

    /**
     * Get information from nodes based on the nodes ids specified. If none are passed, information
     * for all nodes will be returned.
     */
    public NodesInfoRequest(String... nodesIds) {
        super(nodesIds);
        all();
    }

    /**
     * Clears all info flags.
     */
    public NodesInfoRequest clear() {
        requestedMetrics.clear();
        return this;
    }

    /**
     * Sets to return all the data.
     */
    public NodesInfoRequest all() {
        requestedMetrics.addAll(Metrics.allMetrics());
        return this;
    }

    /**
     * Should the node settings be returned.
     */
    public boolean settings() {
        return Metrics.SETTINGS.containedIn(requestedMetrics);
    }

    /**
     * Should the node settings be returned.
     */
    public NodesInfoRequest settings(boolean settings) {
        addOrRemoveMetric(settings, Metrics.SETTINGS.metricName());
        return this;
    }

    /**
     * Should the node OS be returned.
     */
    public boolean os() {
        return Metrics.OS.containedIn(requestedMetrics);
    }

    /**
     * Should the node OS be returned.
     */
    public NodesInfoRequest os(boolean os) {
        addOrRemoveMetric(os, Metrics.OS.metricName());
        return this;
    }

    /**
     * Should the node Process be returned.
     */
    public boolean process() {
        return Metrics.PROCESS.containedIn(requestedMetrics);
    }

    /**
     * Should the node Process be returned.
     */
    public NodesInfoRequest process(boolean process) {
        addOrRemoveMetric(process, Metrics.PROCESS.metricName());
        return this;
    }

    /**
     * Should the node JVM be returned.
     */
    public boolean jvm() {
        return Metrics.JVM.containedIn(requestedMetrics);
    }

    /**
     * Should the node JVM be returned.
     */
    public NodesInfoRequest jvm(boolean jvm) {
        addOrRemoveMetric(jvm, Metrics.JVM.metricName());
        return this;
    }

    /**
     * Should the node Thread Pool info be returned.
     */
    public boolean threadPool() {
        return Metrics.THREAD_POOL.containedIn(requestedMetrics);
    }

    /**
     * Should the node Thread Pool info be returned.
     */
    public NodesInfoRequest threadPool(boolean threadPool) {
        addOrRemoveMetric(threadPool, Metrics.THREAD_POOL.metricName());
        return this;
    }

    /**
     * Should the node Transport be returned.
     */
    public boolean transport() {
        return Metrics.TRANSPORT.containedIn(requestedMetrics);
    }

    /**
     * Should the node Transport be returned.
     */
    public NodesInfoRequest transport(boolean transport) {
        addOrRemoveMetric(transport, Metrics.TRANSPORT.metricName());
        return this;
    }

    /**
     * Should the node HTTP be returned.
     */
    public boolean http() {
        return Metrics.HTTP.containedIn(requestedMetrics);
    }

    /**
     * Should the node HTTP be returned.
     */
    public NodesInfoRequest http(boolean http) {
        addOrRemoveMetric(http, Metrics.HTTP.metricName());
        return this;
    }

    /**
     * Should information about plugins be returned
     * @param plugins true if you want info
     * @return The request
     */
    public NodesInfoRequest plugins(boolean plugins) {
        addOrRemoveMetric(plugins, Metrics.PLUGINS.metricName());
        return this;
    }

    /**
     * @return true if information about plugins is requested
     */
    public boolean plugins() {
        return Metrics.PLUGINS.containedIn(requestedMetrics);
    }

    /**
     * Should information about ingest be returned
     * @param ingest true if you want info
     */
    public NodesInfoRequest ingest(boolean ingest) {
        addOrRemoveMetric(ingest, Metrics.INGEST.metricName());
        return this;
    }

    /**
     * @return true if information about ingest is requested
     */
    public boolean ingest() {
        return Metrics.INGEST.containedIn(requestedMetrics);
    }

    /**
     * Should information about indices (currently just indexing buffers) be returned
     * @param indices true if you want info
     */
    public NodesInfoRequest indices(boolean indices) {
        addOrRemoveMetric(indices, Metrics.INDICES.metricName());
        return this;
    }

    /**
     * @return true if information about indices (currently just indexing buffers)
     */
    public boolean indices() {
        return Metrics.INDICES.containedIn(requestedMetrics);
    }

    /**
     * Helper method for adding and removing metrics.
     * @param includeMetric Whether or not to include a metric.
     * @param metricName Name of the metric to include or remove.
     */
    private void addOrRemoveMetric(boolean includeMetric, String metricName) {
        if (includeMetric) {
            requestedMetrics.add(metricName);
        } else {
            requestedMetrics.remove(metricName);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_7_7_0)){
            // prior to version 8.x, a NodesInfoRequest was serialized as a list
            // of booleans in a fixed order
            out.writeBoolean(Metrics.SETTINGS.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.OS.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.PROCESS.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.JVM.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.THREAD_POOL.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.TRANSPORT.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.HTTP.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.PLUGINS.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.INGEST.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.INDICES.containedIn(requestedMetrics));
        } else {
            out.writeStringArray(requestedMetrics.toArray(String[]::new));
        }
    }

    /**
     * An enumeration of the "core" sections of metrics that may be requested
     * from the nodes information endpoint. Eventually this list list will be
     * pluggable.
     */
    enum Metrics {
        SETTINGS("settings"),
        OS("os"),
        PROCESS("process"),
        JVM("jvm"),
        THREAD_POOL("threadPool"),
        TRANSPORT("transport"),
        HTTP("http"),
        PLUGINS("plugins"),
        INGEST("ingest"),
        INDICES("indices");

        private String metricName;

        Metrics(String name) {
            this.metricName = name;
        }

        String metricName() {
            return this.metricName;
        }

        boolean containedIn(Set<String> metricNames) {
            return metricNames.contains(this.metricName());
        }

        static Set<String> allMetrics() {
            return Arrays.stream(values()).map(Metrics::metricName).collect(Collectors.toSet());
        }
    }
}
