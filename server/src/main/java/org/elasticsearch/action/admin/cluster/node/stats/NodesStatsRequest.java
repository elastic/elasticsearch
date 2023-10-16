/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A request to get node (cluster) level stats.
 */
public class NodesStatsRequest extends BaseNodesRequest<NodesStatsRequest> {

    private NodesStatsMetrics nodesStatsMetrics;

    public NodesStatsRequest() {
        super((String[]) null);
        nodesStatsMetrics = new NodesStatsMetrics();
    }

    public NodesStatsRequest(StreamInput in) throws IOException {
        super(in);

        nodesStatsMetrics = new NodesStatsMetrics(in);
    }

    /**
     * Get stats from nodes based on the nodes ids specified. If none are passed, stats
     * for all nodes will be returned.
     */
    public NodesStatsRequest(String... nodesIds) {
        super(nodesIds);
        nodesStatsMetrics = new NodesStatsMetrics();
    }

    /**
     * Sets all the request flags.
     */
    public NodesStatsRequest all() {
        this.nodesStatsMetrics.indices().all();
        this.nodesStatsMetrics.requestedMetrics().addAll(NodesStatsMetrics.Metric.allMetrics());
        return this;
    }

    /**
     * Clears all the request flags.
     */
    public NodesStatsRequest clear() {
        this.nodesStatsMetrics.indices().clear();
        this.nodesStatsMetrics.requestedMetrics().clear();
        return this;
    }

    /**
     * Get nodesStatsMetrics.indices(). Handles separately from other metrics because it may or
     * may not have submetrics.
     * @return flags indicating which indices stats to return
     */
    public CommonStatsFlags indices() {
        return nodesStatsMetrics.indices();
    }

    /**
     * Set nodesStatsMetrics.indices(). Handles separately from other metrics because it may or
     * may not involve submetrics.
     * @param indices flags indicating which indices stats to return
     * @return This object, for request chaining.
     */
    public NodesStatsRequest indices(CommonStatsFlags indices) {
        nodesStatsMetrics.setIndices(indices);
        return this;
    }

    /**
     * Should indices stats be returned.
     */
    public NodesStatsRequest indices(boolean indices) {
        if (indices) {
            this.nodesStatsMetrics.indices().all();
        } else {
            this.nodesStatsMetrics.indices().clear();
        }
        return this;
    }

    /**
     * Get the names of requested metrics, excluding indices, which are
     * handled separately.
     */
    public Set<String> requestedMetrics() {
        return Set.copyOf(nodesStatsMetrics.requestedMetrics());
    }

    /**
     * Add metric
     */
    public NodesStatsRequest addMetric(String metric) {
        if (NodesStatsMetrics.Metric.allMetrics().contains(metric) == false) {
            throw new IllegalStateException("Used an illegal metric: " + metric);
        }
        nodesStatsMetrics.requestedMetrics().add(metric);
        return this;
    }

    /**
     * Add an array of metric names
     */
    public NodesStatsRequest addMetrics(String... metrics) {
        // use sorted set for reliable ordering in error messages
        SortedSet<String> metricsSet = new TreeSet<>(Set.of(metrics));
        if (NodesStatsMetrics.Metric.allMetrics().containsAll(metricsSet) == false) {
            metricsSet.removeAll(NodesStatsMetrics.Metric.allMetrics());
            String plural = metricsSet.size() == 1 ? "" : "s";
            throw new IllegalStateException("Used illegal metric" + plural + ": " + metricsSet);
        }
        nodesStatsMetrics.requestedMetrics().addAll(metricsSet);
        return this;
    }

    /**
     * Remove metric
     */
    public NodesStatsRequest removeMetric(String metric) {
        if (NodesStatsMetrics.Metric.allMetrics().contains(metric) == false) {
            throw new IllegalStateException("Used an illegal metric: " + metric);
        }
        nodesStatsMetrics.requestedMetrics().remove(metric);
        return this;
    }

    @Override
    public String getDescription() {
        return Strings.format(
            "nodes=%s, metrics=%s, flags=%s",
            Arrays.toString(nodesIds()),
            nodesStatsMetrics.requestedMetrics().toString(),
            Arrays.toString(nodesStatsMetrics.indices().getFlags())
        );
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return NodesStatsRequest.this.getDescription();
            }
        };
    }

    public boolean includeShardsStats() {
        return nodesStatsMetrics.includeShardsStats();
    }

    public void setIncludeShardsStats(boolean includeShardsStats) {
        nodesStatsMetrics.setIncludeShardsStats(includeShardsStats);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        nodesStatsMetrics.writeTo(out);
    }

}
