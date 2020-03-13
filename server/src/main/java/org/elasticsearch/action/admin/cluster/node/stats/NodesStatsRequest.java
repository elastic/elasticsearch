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
import java.util.Collection;
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

    /**
     * Get indices. Handles separately from other metrics because it may or
     * may not have submetrics.
     * @return flags indicating which indices stats to return
     */
    public CommonStatsFlags indices() {
        return indices;
    }

    /**
     * Set indices. Handles separately from other metrics because it may or
     * may not involve submetrics.
     * @param indices flags indicating which indices stats to return
     * @return This object, for request chaining.
     */
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
     * Get the names of requested metrics, excluding indices, which are
     * handled separately.
     */
    public Set<String> requestedMetrics() {
        return Set.copyOf(requestedMetrics);
    }

    /**
     * Add metric
     */
    public NodesStatsRequest addMetric(String metric) {
        if (Metric.allMetrics().contains(metric) == false) {
            throw new IllegalStateException("Used an illegal metric: " + metric);
        }
        requestedMetrics.add(metric);
        return this;
    }

    /**
     * Add a collection of metrics
     */
    public NodesStatsRequest addMetrics(Collection<String> metrics) {
        if (Metric.allMetrics().containsAll(metrics) == false) {
            throw new IllegalStateException("Used a illegal metrics: " + metrics);
        }
        requestedMetrics.addAll(metrics);
        return this;
    }

    /**
     * Remove metric
     */
    public NodesStatsRequest removeMetric(String metric) {
        if (Metric.allMetrics().contains(metric) == false) {
            throw new IllegalStateException("Used an illegal metric: " + metric);
        }
        requestedMetrics.remove(metric);
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
    public enum Metric {
        OS("os"),
        PROCESS("process"),
        JVM("jvm"),
        THREAD_POOL("thread_pool"),
        FS("fs"),
        TRANSPORT("transport"),
        HTTP("http"),
        BREAKER("breaker"),
        SCRIPT("script"),
        DISCOVERY("discovery"),
        INGEST("ingest"),
        ADAPTIVE_SELECTION("adaptiveSelection"); // TODO: Should this be removed?

        private String metricName;

        Metric(String name) {
            this.metricName = name;
        }

        public String metricName() {
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
