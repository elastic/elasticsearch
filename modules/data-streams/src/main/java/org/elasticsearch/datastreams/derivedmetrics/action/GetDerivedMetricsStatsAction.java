/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Reports what derived metrics are actually costing, broken down by the things an operator can change: the source data stream, the metric
 * within it, and the dimension within that.
 *
 * <p>The state this reads is node-local — every node holds only the series it observed, and nothing about it is coordinated — so this is a
 * nodes fan-out rather than a shard-level broadcast or a master-node read. The audience is nevertheless a data stream audience, so the
 * per-node responses are reduced into a per-data-stream shape before anyone sees them; which node happened to hold a series is an
 * implementation detail of how the write path avoids coordination, not something to make a reader reason about.
 *
 * <p>The whole feature is unreleased and gated behind the {@code derived_metrics_in_data_stream_options} transport version, so there is
 * deliberately <em>no</em> new transport version here and none of the serialization below is conditional on one: a cluster old enough not
 * to understand these messages is a cluster where the action does not exist.
 */
public class GetDerivedMetricsStatsAction {

    public static final ActionType<Response> INSTANCE = new ActionType<>("cluster:monitor/data_stream/derived_metrics/stats");

    private GetDerivedMetricsStatsAction() {/* no instances */}

    public static class Request extends BaseNodesRequest {

        public Request() {
            // every node, because every node holds a different part of the answer
            super((String[]) null);
        }
    }

    /**
     * What one node is asked for. It carries the project because the buffer holds every project's streams together and the resolution can
     * only be done where the request's headers are, which is the coordinating node.
     */
    public static class NodeRequest extends AbstractTransportRequest {

        private final ProjectId project;

        public NodeRequest(ProjectId project) {
            this.project = project;
        }

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            this.project = ProjectId.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            project.writeTo(out);
        }

        public ProjectId project() {
            return project;
        }
    }

    /**
     * Series this node refused, by which budget refused them. Which one it was decides what helps: the node cap says raise the node's
     * budget, the stream cap says go and find the stream, the breaker says the problem is memory rather than cardinality, and the
     * histogram cap says the node is full of distributions rather than full.
     */
    public record Refusals(long atNodeCap, long atStreamCap, long atHistogramCap, long atBreaker) implements Writeable {

        public static final Refusals NONE = new Refusals(0, 0, 0, 0);

        public static Refusals read(StreamInput in) throws IOException {
            return new Refusals(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(atNodeCap);
            out.writeVLong(atStreamCap);
            out.writeVLong(atHistogramCap);
            out.writeVLong(atBreaker);
        }

        public Refusals plus(Refusals other) {
            return new Refusals(
                atNodeCap + other.atNodeCap,
                atStreamCap + other.atStreamCap,
                atHistogramCap + other.atHistogramCap,
                atBreaker + other.atBreaker
            );
        }

        public long total() {
            return atNodeCap + atStreamCap + atHistogramCap + atBreaker;
        }

        void toXContent(XContentBuilder builder) throws IOException {
            builder.startObject("series_dropped");
            builder.field("total", total());
            builder.field("node_cap", atNodeCap);
            builder.field("stream_cap", atStreamCap);
            builder.field("histogram_cap", atHistogramCap);
            builder.field("breaker", atBreaker);
            builder.endObject();
        }
    }

    /**
     * What one dimension of one metric has been seen to hold.
     *
     * @param estimatedDistinctValues an approximation from a HyperLogLog sketch, within a few per cent. It is a floor rather than a
     *                                measurement: a value refused at a series cap is never interned and therefore never counted, so the
     *                                estimate stops climbing once the metric is over budget.
     * @param collapsed               whether the metric has given up breaking down by this dimension because it outgrew
     *                                {@code max_dimension_cardinality}. This is the answer to "which dimension exploded".
     */
    public record DimensionStats(String name, long estimatedDistinctValues, boolean collapsed) implements Writeable {

        public static DimensionStats read(StreamInput in) throws IOException {
            return new DimensionStats(in.readString(), in.readVLong(), in.readBoolean());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(estimatedDistinctValues);
            out.writeBoolean(collapsed);
        }

        /**
         * Combines what two nodes saw of the same dimension by taking the larger estimate, never the sum. Each node's sketch counts the
         * values <em>it</em> observed, and the same value seen on two nodes is one value — summing would multiply the answer by the fleet
         * size, which is the opposite of useful when the question is whether a dimension is worth breaking down by.
         *
         * <p>The larger estimate is itself a lower bound, and it is worth knowing why rather than reading this as exact. Two nodes that
         * saw disjoint sets of values hold a union larger than either, and this reports the larger. Documents are spread across nodes
         * without regard to their dimension values, so in practice each node sees a sample of the same population and the estimates
         * converge — most so for exactly the high-cardinality dimensions the question is about. A dimension correlated with routing would
         * be the exception, and would read low.
         *
         * <p>The sketches are mergeable, so an exact cross-node estimate is available for the cost of putting them on the wire. That is a
         * deliberate trade rather than an oversight: this answers "is this dimension worth breaking down by", where an order of magnitude
         * is the whole of the decision.
         */
        public DimensionStats merge(DimensionStats other) {
            return new DimensionStats(name, Math.max(estimatedDistinctValues, other.estimatedDistinctValues), collapsed || other.collapsed);
        }
    }

    /**
     * What one metric of one data stream costs.
     *
     * @param seriesHeld series buffered right now, summed across nodes. It is a snapshot of live state rather than a total, so it moves
     *                   with the flush cycle and is zero for a metric between buckets.
     * @param bytesHeld  what those series are charged to the derived metrics circuit breaker for
     */
    public record MetricStats(
        String name,
        String interval,
        long seriesHeld,
        long bytesHeld,
        boolean histogram,
        List<DimensionStats> dimensions
    ) implements Writeable {

        public static MetricStats read(StreamInput in) throws IOException {
            return new MetricStats(
                in.readString(),
                in.readOptionalString(),
                in.readVLong(),
                in.readVLong(),
                in.readBoolean(),
                in.readCollectionAsImmutableList(DimensionStats::read)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeOptionalString(interval);
            out.writeVLong(seriesHeld);
            out.writeVLong(bytesHeld);
            out.writeBoolean(histogram);
            out.writeCollection(dimensions);
        }

        void toXContent(XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.field("name", name);
            if (interval != null) {
                builder.field("interval", interval);
            }
            builder.field("type", histogram ? "histogram" : "scalar");
            builder.field("series_held", seriesHeld);
            builder.humanReadableField("bytes_held_in_bytes", "bytes_held", ByteSizeValue.ofBytes(bytesHeld));
            if (dimensions.isEmpty() == false) {
                builder.startArray("dimensions");
                for (DimensionStats dimension : dimensions) {
                    builder.startObject();
                    builder.field("name", dimension.name());
                    builder.field("estimated_distinct_values", dimension.estimatedDistinctValues());
                    builder.field("collapsed", dimension.collapsed());
                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();
        }
    }

    /** What one source data stream's derived metrics cost, across every node that observed writes to it. */
    public record DataStreamStats(
        String name,
        long seriesHeld,
        long histogramSeriesHeld,
        long bytesHeld,
        Refusals refusals,
        long bucketsDropped,
        List<MetricStats> metrics
    ) implements Writeable {

        public static DataStreamStats read(StreamInput in) throws IOException {
            return new DataStreamStats(
                in.readString(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                Refusals.read(in),
                in.readVLong(),
                in.readCollectionAsImmutableList(MetricStats::read)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(seriesHeld);
            out.writeVLong(histogramSeriesHeld);
            out.writeVLong(bytesHeld);
            refusals.writeTo(out);
            out.writeVLong(bucketsDropped);
            out.writeCollection(metrics);
        }

        void toXContent(XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.field("name", name);
            builder.field("series_held", seriesHeld);
            builder.field("histogram_series_held", histogramSeriesHeld);
            builder.humanReadableField("bytes_held_in_bytes", "bytes_held", ByteSizeValue.ofBytes(bytesHeld));
            builder.field("buckets_dropped", bucketsDropped);
            refusals.toXContent(builder);
            builder.startArray("metrics");
            for (MetricStats metric : metrics) {
                metric.toXContent(builder);
            }
            builder.endArray();
            builder.endObject();
        }
    }

    /**
     * The counters that are node-wide rather than attributable to a stream. They stay node-wide on purpose: every one of them would need a
     * map lookup <em>per document</em> on the indexing thread to break down, and that path is measured — an observation against an existing
     * series allocates nothing today. See the class javadoc of {@link TransportGetDerivedMetricsStatsAction}.
     */
    public record NodeTotals(
        long seriesHeld,
        long histogramSeriesHeld,
        long inFlightDocuments,
        Refusals refusals,
        long documentsReadFromIndex,
        long documentsReadFromSource,
        long observationsSkippedMissingValue,
        long observationsSkippedUnreadableSource,
        long dimensionsCollapsed,
        long partialsExhausted,
        long tablesRetired,
        long recoveredAfterRelief,
        long earlyFlushes,
        long documentsRejected,
        long documentsFailed,
        long documentsDroppedForBackpressure,
        long documentsDroppedForIndexingPressure,
        long bucketsDropped,
        long maxBucketsDroppedInACycle
    ) implements Writeable {

        public static final NodeTotals EMPTY = new NodeTotals(0, 0, 0, Refusals.NONE, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        public static NodeTotals read(StreamInput in) throws IOException {
            return new NodeTotals(
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                Refusals.read(in),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong(),
                in.readVLong()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(seriesHeld);
            out.writeVLong(histogramSeriesHeld);
            out.writeVLong(inFlightDocuments);
            refusals.writeTo(out);
            out.writeVLong(documentsReadFromIndex);
            out.writeVLong(documentsReadFromSource);
            out.writeVLong(observationsSkippedMissingValue);
            out.writeVLong(observationsSkippedUnreadableSource);
            out.writeVLong(dimensionsCollapsed);
            out.writeVLong(partialsExhausted);
            out.writeVLong(tablesRetired);
            out.writeVLong(recoveredAfterRelief);
            out.writeVLong(earlyFlushes);
            out.writeVLong(documentsRejected);
            out.writeVLong(documentsFailed);
            out.writeVLong(documentsDroppedForBackpressure);
            out.writeVLong(documentsDroppedForIndexingPressure);
            out.writeVLong(bucketsDropped);
            out.writeVLong(maxBucketsDroppedInACycle);
        }

        public NodeTotals plus(NodeTotals other) {
            return new NodeTotals(
                seriesHeld + other.seriesHeld,
                histogramSeriesHeld + other.histogramSeriesHeld,
                inFlightDocuments + other.inFlightDocuments,
                refusals.plus(other.refusals),
                documentsReadFromIndex + other.documentsReadFromIndex,
                documentsReadFromSource + other.documentsReadFromSource,
                observationsSkippedMissingValue + other.observationsSkippedMissingValue,
                observationsSkippedUnreadableSource + other.observationsSkippedUnreadableSource,
                dimensionsCollapsed + other.dimensionsCollapsed,
                partialsExhausted + other.partialsExhausted,
                tablesRetired + other.tablesRetired,
                recoveredAfterRelief + other.recoveredAfterRelief,
                earlyFlushes + other.earlyFlushes,
                documentsRejected + other.documentsRejected,
                documentsFailed + other.documentsFailed,
                documentsDroppedForBackpressure + other.documentsDroppedForBackpressure,
                documentsDroppedForIndexingPressure + other.documentsDroppedForIndexingPressure,
                bucketsDropped + other.bucketsDropped,
                // a maximum per node, so the cluster answer is the worst node rather than a total that would mean nothing
                Math.max(maxBucketsDroppedInACycle, other.maxBucketsDroppedInACycle)
            );
        }

        void toXContent(XContentBuilder builder) throws IOException {
            builder.field("series_held", seriesHeld);
            builder.field("histogram_series_held", histogramSeriesHeld);
            builder.field("documents_in_flight", inFlightDocuments);
            refusals.toXContent(builder);
            builder.startObject("documents_read");
            builder.field("from_index", documentsReadFromIndex);
            builder.field("from_source", documentsReadFromSource);
            builder.endObject();
            builder.startObject("observations_skipped");
            builder.field("missing_value", observationsSkippedMissingValue);
            builder.field("unreadable_source", observationsSkippedUnreadableSource);
            builder.endObject();
            builder.startObject("documents_dropped");
            builder.field("backpressure", documentsDroppedForBackpressure);
            builder.field("indexing_pressure", documentsDroppedForIndexingPressure);
            builder.field("rejected", documentsRejected);
            builder.field("failed", documentsFailed);
            builder.endObject();
            builder.field("dimensions_collapsed", dimensionsCollapsed);
            builder.field("early_flushes", earlyFlushes);
            builder.field("partials_exhausted", partialsExhausted);
            builder.field("tables_retired", tablesRetired);
            builder.field("recovered_after_relief", recoveredAfterRelief);
        }
    }

    public static class NodeResponse extends BaseNodeResponse {

        private final NodeTotals totals;
        private final List<DataStreamStats> dataStreams;

        public NodeResponse(DiscoveryNode node, NodeTotals totals, List<DataStreamStats> dataStreams) {
            super(node);
            this.totals = totals;
            this.dataStreams = List.copyOf(dataStreams);
        }

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            this.totals = NodeTotals.read(in);
            this.dataStreams = in.readCollectionAsImmutableList(DataStreamStats::read);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            totals.writeTo(out);
            out.writeCollection(dataStreams);
        }

        public NodeTotals totals() {
            return totals;
        }

        public List<DataStreamStats> dataStreams() {
            return dataStreams;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            NodeResponse other = (NodeResponse) o;
            return Objects.equals(getNode(), other.getNode())
                && Objects.equals(totals, other.totals)
                && Objects.equals(dataStreams, other.dataStreams);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getNode(), totals, dataStreams);
        }
    }

    public static class Response extends BaseNodesResponse<NodeResponse> implements ToXContentObject {

        public Response(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readCollectionAsList(NodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            out.writeCollection(nodes);
        }

        /** The node-wide counters of every node that answered, added together. */
        public NodeTotals totals() {
            NodeTotals totals = NodeTotals.EMPTY;
            for (NodeResponse node : getNodes()) {
                totals = totals.plus(node.totals());
            }
            return totals;
        }

        /**
         * The per-node views of one data stream reduced into one, sorted by name so the output is stable between calls.
         *
         * <p>Everything additive is added: a series is held on exactly one node, and a refusal happened on exactly one node. The one
         * exception is a dimension's estimated cardinality, which is combined by taking the largest — see
         * {@link DimensionStats#merge(DimensionStats)}.
         */
        public List<DataStreamStats> dataStreams() {
            Map<String, DataStreamStats> merged = new HashMap<>();
            for (NodeResponse node : getNodes()) {
                for (DataStreamStats stats : node.dataStreams()) {
                    merged.merge(stats.name(), stats, Response::mergeDataStream);
                }
            }
            List<DataStreamStats> sorted = new ArrayList<>(merged.values());
            sorted.sort(Comparator.comparing(DataStreamStats::name));
            return sorted;
        }

        private static DataStreamStats mergeDataStream(DataStreamStats left, DataStreamStats right) {
            Map<String, MetricStats> metrics = new HashMap<>();
            for (MetricStats metric : left.metrics()) {
                metrics.put(metric.name(), metric);
            }
            for (MetricStats metric : right.metrics()) {
                metrics.merge(metric.name(), metric, Response::mergeMetric);
            }
            List<MetricStats> sorted = new ArrayList<>(metrics.values());
            sorted.sort(Comparator.comparing(MetricStats::name));
            return new DataStreamStats(
                left.name(),
                left.seriesHeld() + right.seriesHeld(),
                left.histogramSeriesHeld() + right.histogramSeriesHeld(),
                left.bytesHeld() + right.bytesHeld(),
                left.refusals().plus(right.refusals()),
                left.bucketsDropped() + right.bucketsDropped(),
                sorted
            );
        }

        private static MetricStats mergeMetric(MetricStats left, MetricStats right) {
            Map<String, DimensionStats> dimensions = new HashMap<>();
            for (DimensionStats dimension : left.dimensions()) {
                dimensions.put(dimension.name(), dimension);
            }
            for (DimensionStats dimension : right.dimensions()) {
                dimensions.merge(dimension.name(), dimension, DimensionStats::merge);
            }
            List<DimensionStats> sorted = new ArrayList<>(dimensions.values());
            sorted.sort(Comparator.comparing(DimensionStats::name));
            return new MetricStats(
                left.name(),
                left.interval() != null ? left.interval() : right.interval(),
                left.seriesHeld() + right.seriesHeld(),
                left.bytesHeld() + right.bytesHeld(),
                left.histogram() || right.histogram(),
                sorted
            );
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            List<DataStreamStats> dataStreams = dataStreams();
            builder.startObject();
            builder.field("node_count", getNodes().size());
            totals().toXContent(builder);
            builder.field("data_stream_count", dataStreams.size());
            builder.startArray("data_streams");
            for (DataStreamStats stats : dataStreams) {
                stats.toXContent(builder);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Response other = (Response) o;
            return Objects.equals(getNodes(), other.getNodes()) && Objects.equals(failures(), other.failures());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getNodes(), failures());
        }
    }
}
