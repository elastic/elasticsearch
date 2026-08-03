/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsBuffer;
import org.elasticsearch.datastreams.derivedmetrics.DerivedMetricsService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Collects one node's view of what derived metrics are costing and reduces the fan-out into a per-data-stream answer.
 *
 * <p><strong>Why a nodes action.</strong> Nothing about derived metrics is coordinated: every node buffers only the series it observed and
 * emits its own partials. There is no shard whose stats could be asked for and no master-node state to read, so the only place the answer
 * exists is on each node individually.
 *
 * <p><strong>What is deliberately not broken down by stream.</strong> Everything counted once per <em>document</em> rather than once per
 * series stays node-wide: whether a document's values came from the parsed document or from a second {@code _source} parse, observations
 * skipped for a missing value, documents shed for backpressure or indexing pressure. Attributing those would mean a map lookup per document
 * on the indexing thread, inside the shard's operation permit, on a path where observing an existing series currently allocates nothing at
 * all — a measurable regression traded for a nicer report. The per-series figures below have no such problem, because they are read from
 * state the shedding decision already maintains, or counted only when an observation was already being refused.
 *
 * <p>Reading a dimension's estimated cardinality sums 256 HyperLogLog registers, which is why it happens here — when someone asks — and
 * never on a schedule or on the write path.
 */
public class TransportGetDerivedMetricsStatsAction extends TransportNodesAction<
    GetDerivedMetricsStatsAction.Request,
    GetDerivedMetricsStatsAction.Response,
    GetDerivedMetricsStatsAction.NodeRequest,
    GetDerivedMetricsStatsAction.NodeResponse,
    Void> {

    private final DerivedMetricsService derivedMetrics;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportGetDerivedMetricsStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        DerivedMetricsService derivedMetrics,
        ProjectResolver projectResolver
    ) {
        super(
            GetDerivedMetricsStatsAction.INSTANCE.name(),
            clusterService,
            transportService,
            actionFilters,
            GetDerivedMetricsStatsAction.NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.derivedMetrics = derivedMetrics;
        this.projectResolver = projectResolver;
    }

    @Override
    protected GetDerivedMetricsStatsAction.Response newResponse(
        GetDerivedMetricsStatsAction.Request request,
        List<GetDerivedMetricsStatsAction.NodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new GetDerivedMetricsStatsAction.Response(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected GetDerivedMetricsStatsAction.NodeRequest newNodeRequest(GetDerivedMetricsStatsAction.Request request) {
        // Resolved here rather than on each node: this runs on the coordinating node, which is the only place the request's project
        // headers are in scope. A node's buffer holds every project's streams together, so without this one tenant would see another's.
        return new GetDerivedMetricsStatsAction.NodeRequest(projectResolver.getProjectId());
    }

    @Override
    protected GetDerivedMetricsStatsAction.NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new GetDerivedMetricsStatsAction.NodeResponse(in);
    }

    @Override
    protected GetDerivedMetricsStatsAction.NodeResponse nodeOperation(GetDerivedMetricsStatsAction.NodeRequest request, Task task) {
        return new GetDerivedMetricsStatsAction.NodeResponse(transportService.getLocalNode(), totals(), dataStreams(request.project()));
    }

    private GetDerivedMetricsStatsAction.NodeTotals totals() {
        return new GetDerivedMetricsStatsAction.NodeTotals(
            derivedMetrics.bufferedSeries(),
            derivedMetrics.histogramSeries(),
            derivedMetrics.inFlightDocuments(),
            new GetDerivedMetricsStatsAction.Refusals(
                derivedMetrics.droppedSeriesAtNodeCap(),
                derivedMetrics.droppedSeriesAtStreamCap(),
                derivedMetrics.droppedSeriesAtHistogramCap(),
                derivedMetrics.droppedSeriesAtBreaker()
            ),
            derivedMetrics.documentsReadFromIndex(),
            derivedMetrics.documentsReadFromSource(),
            derivedMetrics.skippedForMissingValue(),
            derivedMetrics.skippedForUnreadableSource(),
            derivedMetrics.dimensionsCollapsed(),
            derivedMetrics.partialsExhausted(),
            derivedMetrics.tablesRetired(),
            derivedMetrics.recoveredAfterRelief(),
            derivedMetrics.earlyFlushes(),
            derivedMetrics.emissionRejections(),
            derivedMetrics.emissionFailures(),
            derivedMetrics.droppedForBackpressure(),
            derivedMetrics.droppedForIndexingPressure(),
            derivedMetrics.bucketsDropped(),
            derivedMetrics.maxBucketsDroppedInACycle()
        );
    }

    /**
     * Builds this node's per-stream view out of three independent sources, because they do not cover the same set of metrics: what is
     * buffered right now, what has ever been refused, and what each dimension has been seen to hold. A metric between buckets is buffering
     * nothing but its dimension sketches still know what its dimensions did, which is exactly the state an operator asks about after a
     * cardinality problem rather than during one.
     */
    private List<GetDerivedMetricsStatsAction.DataStreamStats> dataStreams(ProjectId project) {
        Map<String, StreamAccumulator> streams = new HashMap<>();
        for (DerivedMetricsBuffer.MetricSnapshot snapshot : derivedMetrics.metricSnapshots()) {
            if (project.equals(snapshot.project()) == false) {
                continue;
            }
            StreamAccumulator stream = streams.computeIfAbsent(snapshot.sourceDataStream(), StreamAccumulator::new);
            MetricAccumulator metric = stream.metric(snapshot.metric());
            metric.interval = snapshot.interval();
            metric.histogram = snapshot.histogram();
            metric.seriesHeld += snapshot.seriesHeld();
            metric.bytesHeld += snapshot.bytesHeld();
            stream.seriesHeld += snapshot.seriesHeld();
            stream.bytesHeld += snapshot.bytesHeld();
            if (snapshot.histogram()) {
                stream.histogramSeriesHeld += snapshot.seriesHeld();
            }
        }
        for (DerivedMetricsBuffer.StreamRefusals refused : derivedMetrics.streamRefusals()) {
            if (project.equals(refused.project()) == false) {
                continue;
            }
            StreamAccumulator stream = streams.computeIfAbsent(refused.sourceDataStream(), StreamAccumulator::new);
            stream.refusals = new GetDerivedMetricsStatsAction.Refusals(
                refused.atNodeCap(),
                refused.atStreamCap(),
                refused.atHistogramCap(),
                refused.atBreaker()
            );
            stream.bucketsDropped = refused.bucketsDropped();
        }
        for (DerivedMetricsBuffer.DimensionCardinality cardinality : derivedMetrics.dimensionCardinalities()) {
            if (project.equals(cardinality.project()) == false) {
                continue;
            }
            StreamAccumulator stream = streams.computeIfAbsent(cardinality.sourceDataStream(), StreamAccumulator::new);
            stream.metric(cardinality.metric()).dimensions.add(
                new GetDerivedMetricsStatsAction.DimensionStats(
                    cardinality.dimension(),
                    cardinality.estimatedValues(),
                    cardinality.collapsed()
                )
            );
        }
        List<GetDerivedMetricsStatsAction.DataStreamStats> stats = new ArrayList<>(streams.size());
        for (StreamAccumulator stream : streams.values()) {
            stats.add(stream.build());
        }
        stats.sort(Comparator.comparing(GetDerivedMetricsStatsAction.DataStreamStats::name));
        return stats;
    }

    /**
     * One source data stream while it is being assembled. A mutable holder rather than repeated rebuilding of the immutable record, because
     * the three sources above each contribute a different part of it.
     */
    private static final class StreamAccumulator {

        private final String name;
        private final Map<String, MetricAccumulator> metrics = new LinkedHashMap<>();
        private long seriesHeld;
        private long histogramSeriesHeld;
        private long bytesHeld;
        private GetDerivedMetricsStatsAction.Refusals refusals = GetDerivedMetricsStatsAction.Refusals.NONE;
        private long bucketsDropped;

        StreamAccumulator(String name) {
            this.name = name;
        }

        /** A metric's name is unique within a stream — one metric is accumulated at exactly one interval — so it is the whole key here. */
        MetricAccumulator metric(String metric) {
            return metrics.computeIfAbsent(metric, MetricAccumulator::new);
        }

        GetDerivedMetricsStatsAction.DataStreamStats build() {
            List<GetDerivedMetricsStatsAction.MetricStats> built = new ArrayList<>(metrics.size());
            for (MetricAccumulator metric : metrics.values()) {
                built.add(metric.build());
            }
            built.sort(Comparator.comparing(GetDerivedMetricsStatsAction.MetricStats::name));
            return new GetDerivedMetricsStatsAction.DataStreamStats(
                name,
                seriesHeld,
                histogramSeriesHeld,
                bytesHeld,
                refusals,
                bucketsDropped,
                built
            );
        }
    }

    private static final class MetricAccumulator {

        private final String name;
        private final List<GetDerivedMetricsStatsAction.DimensionStats> dimensions = new ArrayList<>();
        private String interval;
        private boolean histogram;
        private long seriesHeld;
        private long bytesHeld;

        MetricAccumulator(String name) {
            this.name = name;
        }

        GetDerivedMetricsStatsAction.MetricStats build() {
            dimensions.sort(Comparator.comparing(GetDerivedMetricsStatsAction.DimensionStats::name));
            return new GetDerivedMetricsStatsAction.MetricStats(name, interval, seriesHeld, bytesHeld, histogram, List.copyOf(dimensions));
        }
    }
}
