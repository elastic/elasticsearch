/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters.Metric;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A request to get node (cluster) level stats.
 */
public class NodesStatsRequest extends BaseNodesRequest<NodesStatsRequest> {

    private final NodesStatsRequestParameters nodesStatsRequestParameters;

    public NodesStatsRequest() {
        super((String[]) null);
        nodesStatsRequestParameters = new NodesStatsRequestParameters();
    }

    /**
     * Get stats from nodes based on the nodes ids specified. If none are passed, stats
     * for all nodes will be returned.
     */
    public NodesStatsRequest(String... nodesIds) {
        this(new NodesStatsRequestParameters(), nodesIds);
    }

    public NodesStatsRequest(NodesStatsRequestParameters nodesStatsRequestParameters, String... nodesIds) {
        super(nodesIds);
        this.nodesStatsRequestParameters = nodesStatsRequestParameters;
    }

    /**
     * Sets all the request flags.
     */
    public NodesStatsRequest all() {
        this.nodesStatsRequestParameters.indices().all();
        this.nodesStatsRequestParameters.requestedMetrics().addAll(Metric.ALL);
        return this;
    }

    /**
     * Clears all the request flags.
     */
    public NodesStatsRequest clear() {
        this.nodesStatsRequestParameters.indices().clear();
        this.nodesStatsRequestParameters.requestedMetrics().clear();
        return this;
    }

    /**
     * Get nodesStatsMetrics.indices(). Handles separately from other metrics because it may or
     * may not have submetrics.
     * @return flags indicating which indices stats to return
     */
    public CommonStatsFlags indices() {
        return nodesStatsRequestParameters.indices();
    }

    /**
     * Set nodesStatsMetrics.indices(). Handles separately from other metrics because it may or
     * may not involve submetrics.
     * @param indices flags indicating which indices stats to return
     * @return This object, for request chaining.
     */
    public NodesStatsRequest indices(CommonStatsFlags indices) {
        nodesStatsRequestParameters.setIndices(indices);
        return this;
    }

    /**
     * Should indices stats be returned.
     */
    public NodesStatsRequest indices(boolean indices) {
        if (indices) {
            this.nodesStatsRequestParameters.indices().all();
        } else {
            this.nodesStatsRequestParameters.indices().clear();
        }
        return this;
    }

    /**
     * Get the names of requested metrics, excluding indices, which are
     * handled separately.
     */
    public Set<Metric> requestedMetrics() {
        return Set.copyOf(nodesStatsRequestParameters.requestedMetrics());
    }

    /**
     * Add metric
     */
    public NodesStatsRequest addMetric(Metric metric) {
        nodesStatsRequestParameters.requestedMetrics().add(metric);
        return this;
    }

    /**
     * Add an array of metric names
     */
    public NodesStatsRequest addMetrics(Metric... metrics) {
        for (var metric : metrics) {
            nodesStatsRequestParameters.requestedMetrics().add(metric);
        }
        return this;
    }

    public NodesStatsRequest addMetrics(List<Metric> metrics) {
        nodesStatsRequestParameters.requestedMetrics().addAll(metrics);
        return this;
    }

    /**
     * Remove metric
     */
    public NodesStatsRequest removeMetric(Metric metric) {
        nodesStatsRequestParameters.requestedMetrics().remove(metric);
        return this;
    }

    @Override
    public String getDescription() {
        return Strings.format(
            "nodes=%s, metrics=%s, flags=%s",
            Arrays.toString(nodesIds()),
            nodesStatsRequestParameters.requestedMetrics().toString(),
            Arrays.toString(nodesStatsRequestParameters.indices().getFlags())
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
        return nodesStatsRequestParameters.includeShardsStats();
    }

    public void setIncludeShardsStats(boolean includeShardsStats) {
        nodesStatsRequestParameters.setIncludeShardsStats(includeShardsStats);
    }

    public NodesStatsRequestParameters getNodesStatsRequestParameters() {
        return nodesStatsRequestParameters;
    }
}
