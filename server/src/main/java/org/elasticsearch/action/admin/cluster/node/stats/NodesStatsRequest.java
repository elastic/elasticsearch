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

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A request to get node (cluster) level stats.
 */
public class NodesStatsRequest extends BaseNodesRequest<NodesStatsRequest> {

    private CommonStatsFlags indices = new CommonStatsFlags();
    private final Set<String> requestedMetrics = new HashSet<>();

    public NodesStatsRequest() {
        super((String[]) null);
    }

    public NodesStatsRequest(StreamInput in) throws IOException {
        super(in);

        indices = new CommonStatsFlags(in);
        requestedMetrics.clear();
        if (in.getVersion().before(Version.V_8_0_0)) {
            addOrRemoveMetric(in.readBoolean(), Metrics.OS.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.PROCESS.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.JVM.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.THREAD_POOL.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.FS.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.TRANSPORT.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.HTTP.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.BREAKER.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.SCRIPT.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.DISCOVERY.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.INGEST.metricName());
            addOrRemoveMetric(in.readBoolean(), Metrics.ADAPTIVE_SELECTION.metricName());
        } else {
            requestedMetrics.addAll(in.readStringList());
        }
    }

    /**
     * Get stats from nodes based on the nodes ids specified. If none are passed, stats
     * for all nodes will be returned.
     */
    public NodesStatsRequest(String... nodesIds) {
        super(nodesIds);
    }

    /**
     * Sets all the request flags.
     */
    public NodesStatsRequest all() {
        this.indices.all();
        this.requestedMetrics.addAll(Metrics.allMetrics());
        return this;
    }

    /**
     * Clears all the request flags.
     */
    public NodesStatsRequest clear() {
        this.indices.clear();
        this.requestedMetrics.clear();
        return this;
    }

    public CommonStatsFlags indices() {
        return indices;
    }

    public NodesStatsRequest indices(CommonStatsFlags indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Should indices stats be returned.
     */
    public NodesStatsRequest indices(boolean indices) {
        if (indices) {
            this.indices.all();
        } else {
            this.indices.clear();
        }
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
    public NodesStatsRequest os(boolean os) {
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
    public NodesStatsRequest process(boolean process) {
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
    public NodesStatsRequest jvm(boolean jvm) {
        addOrRemoveMetric(jvm, Metrics.JVM.metricName());
        return this;
    }

    /**
     * Should the node Thread Pool be returned.
     */
    public boolean threadPool() {
        return Metrics.THREAD_POOL.containedIn(requestedMetrics);
    }

    /**
     * Should the node Thread Pool be returned.
     */
    public NodesStatsRequest threadPool(boolean threadPool) {
        addOrRemoveMetric(threadPool, Metrics.THREAD_POOL.metricName());
        return this;
    }

    /**
     * Should the node file system stats be returned.
     */
    public boolean fs() {
        return Metrics.FS.containedIn(requestedMetrics);
    }

    /**
     * Should the node file system stats be returned.
     */
    public NodesStatsRequest fs(boolean fs) {
        addOrRemoveMetric(fs, Metrics.FS.metricName());
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
    public NodesStatsRequest transport(boolean transport) {
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
    public NodesStatsRequest http(boolean http) {
        addOrRemoveMetric(http, Metrics.HTTP.metricName());
        return this;
    }

    public boolean breaker() {
        return Metrics.BREAKER.containedIn(requestedMetrics);
    }

    /**
     * Should the node's circuit breaker stats be returned.
     */
    public NodesStatsRequest breaker(boolean breaker) {
        addOrRemoveMetric(breaker, Metrics.BREAKER.metricName());
        return this;
    }

    public boolean script() {
        return Metrics.SCRIPT.containedIn(requestedMetrics);
    }

    public NodesStatsRequest script(boolean script) {
        addOrRemoveMetric(script, Metrics.SCRIPT.metricName());
        return this;
    }


    public boolean discovery() {
        return Metrics.DISCOVERY.containedIn(requestedMetrics);
    }

    /**
     * Should the node's discovery stats be returned.
     */
    public NodesStatsRequest discovery(boolean discovery) {
        addOrRemoveMetric(discovery, Metrics.DISCOVERY.metricName());
        return this;
    }

    public boolean ingest() {
        return Metrics.INGEST.containedIn(requestedMetrics);
    }

    /**
     * Should ingest statistics be returned.
     */
    public NodesStatsRequest ingest(boolean ingest) {
        addOrRemoveMetric(ingest, Metrics.INGEST.metricName());
        return this;
    }

    public boolean adaptiveSelection() {
        return Metrics.ADAPTIVE_SELECTION.containedIn(requestedMetrics);
    }

    /**
     * Should adaptiveSelection statistics be returned.
     */
    public NodesStatsRequest adaptiveSelection(boolean adaptiveSelection) {
        addOrRemoveMetric(adaptiveSelection, Metrics.ADAPTIVE_SELECTION.metricName());
        return this;
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
        indices.writeTo(out);
        if (out.getVersion().before(Version.V_8_0_0)) {
            out.writeBoolean(Metrics.OS.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.PROCESS.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.JVM.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.THREAD_POOL.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.FS.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.TRANSPORT.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.HTTP.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.BREAKER.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.SCRIPT.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.DISCOVERY.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.INGEST.containedIn(requestedMetrics));
            out.writeBoolean(Metrics.ADAPTIVE_SELECTION.containedIn(requestedMetrics));
        } else {
            out.writeStringArray(requestedMetrics.toArray(String[]::new));
        }
    }

    /**
     * An enumeration of the "core" sections of metrics that may be requested
     * from the nodes stats endpoint. Eventually this list will be pluggable.
     */
    private enum Metrics {
        OS("os"),
        PROCESS("process"),
        JVM("jvm"),
        THREAD_POOL("threadPool"),
        FS("fs"),
        TRANSPORT("transport"),
        HTTP("http"),
        BREAKER("breaker"),
        SCRIPT("script"),
        DISCOVERY("discovery"),
        INGEST("ingest"),
        ADAPTIVE_SELECTION("adaptiveSelection");

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
