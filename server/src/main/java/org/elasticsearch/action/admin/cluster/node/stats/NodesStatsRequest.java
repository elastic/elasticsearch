/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
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
            optionallyAddMetric(in.readBoolean(), Metric.OS.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.PROCESS.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.JVM.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.THREAD_POOL.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.FS.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.TRANSPORT.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.HTTP.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.BREAKER.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.SCRIPT.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.DISCOVERY.metricName());
            optionallyAddMetric(in.readBoolean(), Metric.INGEST.metricName());
            if (in.getVersion().onOrAfter(Version.V_6_1_0)) {
                optionallyAddMetric(in.readBoolean(), Metric.ADAPTIVE_SELECTION.metricName());
            }
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
        return new HashSet<>(requestedMetrics);
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
     * Add an array of metric names
     */
    public NodesStatsRequest addMetrics(String... metrics) {
        // use sorted set for reliable ordering in error messages
        SortedSet<String> metricsSet = new TreeSet<>(Arrays.asList(metrics));
        if (Metric.allMetrics().containsAll(metricsSet) == false) {
            metricsSet.removeAll(Metric.allMetrics());
            String plural = metricsSet.size() == 1 ? "" : "s";
            throw new IllegalStateException("Used illegal metric" + plural + ": " + metricsSet);
        }
        requestedMetrics.addAll(metricsSet);
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
     * Helper method for adding metrics during deserialization.
     * @param includeMetric Whether or not to include a metric.
     * @param metricName Name of the metric to add.
     */
    private void optionallyAddMetric(boolean includeMetric, String metricName) {
        if (includeMetric) {
            requestedMetrics.add(metricName);
        }
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers);
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
            if (out.getVersion().onOrAfter(Version.V_6_1_0)) {
                out.writeBoolean(Metric.ADAPTIVE_SELECTION.containedIn(requestedMetrics));
            }
        } else {
            out.writeStringArray(requestedMetrics.toArray(new String[0]));
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
        ADAPTIVE_SELECTION("adaptive_selection"),
        SCRIPT_CACHE("script_cache"),
        INDEXING_PRESSURE("indexing_pressure"),;

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
