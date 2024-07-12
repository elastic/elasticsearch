/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.apache.lucene.util.Counter;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;

import java.util.Objects;
import java.util.Optional;


public final class ClusterManagerMetrics {

    private static final String LATENCY_METRIC_UNIT_MS = "ms";
    private static final String COUNTER_METRICS_UNIT = "1";

    public final Histogram clusterStateAppliersHistogram;
    public final Histogram clusterStateListenersHistogram;
    public final Histogram rerouteHistogram;
    public final Histogram clusterStateComputeHistogram;
    public final Histogram clusterStatePublishHistogram;

    public final Counter leaderCheckFailureCounter;
    public final Counter followerChecksFailureCounter;

    public ClusterManagerMetrics(MetricsRegistry metricsRegistry) {
        clusterStateAppliersHistogram = metricsRegistry.createHistogram(
            "cluster.state.appliers.latency",
            "Histogram for tracking the latency of cluster state appliers",
            LATENCY_METRIC_UNIT_MS
        );
        clusterStateListenersHistogram = metricsRegistry.createHistogram(
            "cluster.state.listeners.latency",
            "Histogram for tracking the latency of cluster state listeners",
            LATENCY_METRIC_UNIT_MS
        );
        rerouteHistogram = metricsRegistry.createHistogram(
            "allocation.reroute.latency",
            "Histogram for recording latency of shard re-routing",
            LATENCY_METRIC_UNIT_MS
        );
        clusterStateComputeHistogram = metricsRegistry.createHistogram(
            "cluster.state.new.compute.latency",
            "Histogram for recording time taken to compute new cluster state",
            LATENCY_METRIC_UNIT_MS
        );
        clusterStatePublishHistogram = metricsRegistry.createHistogram(
            "cluster.state.publish.success.latency",
            "Histogram for recording time taken to publish a new cluster state",
            LATENCY_METRIC_UNIT_MS
        );
        followerChecksFailureCounter = metricsRegistry.createCounter(
            "followers.checker.failure.count",
            "Counter for number of failed follower checks",
            COUNTER_METRICS_UNIT
        );
        leaderCheckFailureCounter = metricsRegistry.createCounter(
            "leader.checker.failure.count",
            "Counter for number of failed leader checks",
            COUNTER_METRICS_UNIT
        );
    }



    public void recordLatency(Histogram histogram, Double value, Optional<Tags> tags) {
        if (Objects.isNull(tags) || tags.isEmpty()) {
            histogram.record(value, tags.get());
            return;
        }
        histogram.record(value, tags.get());
    }

    public void incrementCounter(Counter counter, long value) {
        incrementCounter(counter, value, Optional.empty());
    }

    public void incrementCounter(Counter counter, long value, Optional<Tags> tags) {
        if (Objects.isNull(tags) || tags.isEmpty()) {
            counter.addAndGet(value);
            return;
        }
        counter.addAndGet(value);
    }

    public void recordLatency(Histogram histogram, double value, double val) {
        Tags tags = null;
        histogram.record(value, tags);
    }
}
