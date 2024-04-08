/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A request to get node (cluster) level information.
 */
public final class NodesInfoRequest extends BaseNodesRequest<NodesInfoRequest> {

    private final NodesInfoMetrics nodesInfoMetrics;

    /**
     * Create a new NodeInfoRequest from a {@link StreamInput} object.
     *
     * @param in A stream input object.
     * @throws IOException if the stream cannot be deserialized.
     */
    public NodesInfoRequest(StreamInput in) throws IOException {
        super(in);
        nodesInfoMetrics = new NodesInfoMetrics(in);
    }

    /**
     * Get information from nodes based on the nodes ids specified. If none are passed, information
     * for all nodes will be returned.
     */
    public NodesInfoRequest(String... nodesIds) {
        super(nodesIds);
        nodesInfoMetrics = new NodesInfoMetrics();
        all();
    }

    /**
     * Clears all info flags.
     */
    public NodesInfoRequest clear() {
        nodesInfoMetrics.requestedMetrics().clear();
        return this;
    }

    /**
     * Sets to return all the data.
     */
    public NodesInfoRequest all() {
        nodesInfoMetrics.requestedMetrics().addAll(NodesInfoMetrics.Metric.allMetrics());
        return this;
    }

    /**
     * Get the names of requested metrics
     */
    public Set<String> requestedMetrics() {
        return Set.copyOf(nodesInfoMetrics.requestedMetrics());
    }

    /**
     * Add metric
     */
    public NodesInfoRequest addMetric(String metric) {
        if (NodesInfoMetrics.Metric.allMetrics().contains(metric) == false) {
            throw new IllegalStateException("Used an illegal metric: " + metric);
        }
        nodesInfoMetrics.requestedMetrics().add(metric);
        return this;
    }

    /**
     * Add multiple metrics
     */
    public NodesInfoRequest addMetrics(String... metrics) {
        SortedSet<String> metricsSet = new TreeSet<>(Set.of(metrics));
        return addMetrics(metricsSet);
    }

    /**
     * Add multiple metrics
     */
    public NodesInfoRequest addMetrics(Set<String> metricsSet) {
        if (NodesInfoMetrics.Metric.allMetrics().containsAll(metricsSet) == false) {
            metricsSet.removeAll(NodesInfoMetrics.Metric.allMetrics());
            String plural = metricsSet.size() == 1 ? "" : "s";
            throw new IllegalStateException("Used illegal metric" + plural + ": " + metricsSet);
        }
        nodesInfoMetrics.requestedMetrics().addAll(metricsSet);
        return this;
    }

    /**
     * Remove metric
     */
    public NodesInfoRequest removeMetric(String metric) {
        if (NodesInfoMetrics.Metric.allMetrics().contains(metric) == false) {
            throw new IllegalStateException("Used an illegal metric: " + metric);
        }
        nodesInfoMetrics.requestedMetrics().remove(metric);
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        nodesInfoMetrics.writeTo(out);
    }

    public NodesInfoMetrics getNodesInfoMetrics() {
        return nodesInfoMetrics;
    }
}
