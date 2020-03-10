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
        if (in.getVersion().before(Version.V_7_7_0)) {
            addOrRemoveMetric(in.readBoolean(), Metric.OS.metricName());
            addOrRemoveMetric(in.readBoolean(), Metric.PROCESS.metricName());
            addOrRemoveMetric(in.readBoolean(), Metric.JVM.metricName());
            addOrRemoveMetric(in.readBoolean(), Metric.THREAD_POOL.metricName());
            addOrRemoveMetric(in.readBoolean(), Metric.FS.metricName());
            addOrRemoveMetric(in.readBoolean(), Metric.TRANSPORT.metricName());
            addOrRemoveMetric(in.readBoolean(), Metric.HTTP.metricName());
            addOrRemoveMetric(in.readBoolean(), Metric.BREAKER.metricName());
            addOrRemoveMetric(in.readBoolean(), Metric.SCRIPT.metricName());
            addOrRemoveMetric(in.readBoolean(), Metric.DISCOVERY.metricName());
            addOrRemoveMetric(in.readBoolean(), Metric.INGEST.metricName());
            addOrRemoveMetric(in.readBoolean(), Metric.ADAPTIVE_SELECTION.metricName());
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
        this.requestedMetrics.addAll(Metric.allMetrics());
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
        return Metric.OS.containedIn(requestedMetrics);
    }

    /**
     * Should the node OS be returned.
     */
    public NodesStatsRequest os(boolean os) {
        addOrRemoveMetric(os, Metric.OS.metricName());
        return this;
    }

    /**
     * Should the node Process be returned.
     */
    public boolean process() {
        return Metric.PROCESS.containedIn(requestedMetrics);
    }

    /**
     * Should the node Process be returned.
     */
    public NodesStatsRequest process(boolean process) {
        addOrRemoveMetric(process, Metric.PROCESS.metricName());
        return this;
    }

    /**
     * Should the node JVM be returned.
     */
    public boolean jvm() {
        return Metric.JVM.containedIn(requestedMetrics);
    }

    /**
     * Should the node JVM be returned.
     */
    public NodesStatsRequest jvm(boolean jvm) {
        addOrRemoveMetric(jvm, Metric.JVM.metricName());
        return this;
    }

    /**
     * Should the node Thread Pool be returned.
     */
    public boolean threadPool() {
        return Metric.THREAD_POOL.containedIn(requestedMetrics);
    }

    /**
     * Should the node Thread Pool be returned.
     */
    public NodesStatsRequest threadPool(boolean threadPool) {
        addOrRemoveMetric(threadPool, Metric.THREAD_POOL.metricName());
        return this;
    }

    /**
     * Should the node file system stats be returned.
     */
    public boolean fs() {
        return Metric.FS.containedIn(requestedMetrics);
    }

    /**
     * Should the node file system stats be returned.
     */
    public NodesStatsRequest fs(boolean fs) {
        addOrRemoveMetric(fs, Metric.FS.metricName());
        return this;
    }

    /**
     * Should the node Transport be returned.
     */
    public boolean transport() {
        return Metric.TRANSPORT.containedIn(requestedMetrics);
    }

    /**
     * Should the node Transport be returned.
     */
    public NodesStatsRequest transport(boolean transport) {
        addOrRemoveMetric(transport, Metric.TRANSPORT.metricName());
        return this;
    }

    /**
     * Should the node HTTP be returned.
     */
    public boolean http() {
        return Metric.HTTP.containedIn(requestedMetrics);
    }

    /**
     * Should the node HTTP be returned.
     */
    public NodesStatsRequest http(boolean http) {
        addOrRemoveMetric(http, Metric.HTTP.metricName());
        return this;
    }

    public boolean breaker() {
        return Metric.BREAKER.containedIn(requestedMetrics);
    }

    /**
     * Should the node's circuit breaker stats be returned.
     */
    public NodesStatsRequest breaker(boolean breaker) {
        addOrRemoveMetric(breaker, Metric.BREAKER.metricName());
        return this;
    }

    public boolean script() {
        return Metric.SCRIPT.containedIn(requestedMetrics);
    }

    public NodesStatsRequest script(boolean script) {
        addOrRemoveMetric(script, Metric.SCRIPT.metricName());
        return this;
    }


    public boolean discovery() {
        return Metric.DISCOVERY.containedIn(requestedMetrics);
    }

    /**
     * Should the node's discovery stats be returned.
     */
    public NodesStatsRequest discovery(boolean discovery) {
        addOrRemoveMetric(discovery, Metric.DISCOVERY.metricName());
        return this;
    }

    public boolean ingest() {
        return Metric.INGEST.containedIn(requestedMetrics);
    }

    /**
     * Should ingest statistics be returned.
     */
    public NodesStatsRequest ingest(boolean ingest) {
        addOrRemoveMetric(ingest, Metric.INGEST.metricName());
        return this;
    }

    public boolean adaptiveSelection() {
        return Metric.ADAPTIVE_SELECTION.containedIn(requestedMetrics);
    }

    /**
     * Should adaptiveSelection statistics be returned.
     */
    public NodesStatsRequest adaptiveSelection(boolean adaptiveSelection) {
        addOrRemoveMetric(adaptiveSelection, Metric.ADAPTIVE_SELECTION.metricName());
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
        if (out.getVersion().before(Version.V_7_7_0)) {
            out.writeBoolean(Metric.OS.containedIn(requestedMetrics));
            out.writeBoolean(Metric.PROCESS.containedIn(requestedMetrics));
            out.writeBoolean(Metric.JVM.containedIn(requestedMetrics));
            out.writeBoolean(Metric.THREAD_POOL.containedIn(requestedMetrics));
            out.writeBoolean(Metric.FS.containedIn(requestedMetrics));
            out.writeBoolean(Metric.TRANSPORT.containedIn(requestedMetrics));
            out.writeBoolean(Metric.HTTP.containedIn(requestedMetrics));
            out.writeBoolean(Metric.BREAKER.containedIn(requestedMetrics));
            out.writeBoolean(Metric.SCRIPT.containedIn(requestedMetrics));
            out.writeBoolean(Metric.DISCOVERY.containedIn(requestedMetrics));
            out.writeBoolean(Metric.INGEST.containedIn(requestedMetrics));
            out.writeBoolean(Metric.ADAPTIVE_SELECTION.containedIn(requestedMetrics));
        } else {
            out.writeStringArray(requestedMetrics.toArray(String[]::new));
        }
    }

    /**
     * An enumeration of the "core" sections of metrics that may be requested
     * from the nodes stats endpoint. Eventually this list will be pluggable.
     */
    private enum Metric {
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

        Metric(String name) {
            this.metricName = name;
        }

        String metricName() {
            return this.metricName;
        }

        boolean containedIn(Set<String> metricNames) {
            return metricNames.contains(this.metricName());
        }

        static Set<String> allMetrics() {
            return Arrays.stream(values()).map(Metric::metricName).collect(Collectors.toSet());
        }
    }
}
