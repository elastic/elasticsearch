/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.analytics.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class AnalyticsStatsAction extends ActionType<AnalyticsStatsAction.Response> {
    public static final AnalyticsStatsAction INSTANCE = new AnalyticsStatsAction();
    public static final String NAME = "cluster:monitor/xpack/analytics/stats";

    private AnalyticsStatsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends BaseNodesRequest<Request> implements ToXContentObject {

        public Request() {
            super((String[]) null);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            // Nothing to hash atm, so just use the action name
            return Objects.hashCode(NAME);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            return true;
        }
    }

    public static class NodeRequest extends TransportRequest {
        public NodeRequest(StreamInput in) throws IOException {
            super(in);
        }

        public NodeRequest(Request request) {

        }
    }

    public static class Response extends BaseNodesResponse<NodeResponse> implements Writeable, ToXContentObject {
        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readList(NodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            out.writeList(nodes);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray("stats");
            for (NodeResponse node : getNodes()) {
                node.toXContent(builder, params);
            }
            builder.endArray();

            return builder;
        }
    }

    public static class NodeResponse extends BaseNodeResponse implements ToXContentObject {
        static final ParseField BOXPLOT_USAGE = new ParseField("boxplot_usage");
        static final ParseField CUMULATIVE_CARDINALITY_USAGE = new ParseField("cumulative_cardinality_usage");
        static final ParseField STRING_STATS_USAGE = new ParseField("string_stats_usage");
        static final ParseField TOP_METRICS_USAGE = new ParseField("top_metrics_usage");
        static final ParseField T_TEST_USAGE = new ParseField("t_test_usage");

        private final long boxplotUsage;
        private final long cumulativeCardinalityUsage;
        private final long stringStatsUsage;
        private final long topMetricsUsage;
        private final long ttestUsage;

        public NodeResponse(DiscoveryNode node, long boxplotUsage, long cumulativeCardinalityUsage, long stringStatsUsage,
                long topMetricsUsage, long ttestUsage) {
            super(node);
            this.boxplotUsage = boxplotUsage;
            this.cumulativeCardinalityUsage = cumulativeCardinalityUsage;
            this.stringStatsUsage = stringStatsUsage;
            this.topMetricsUsage = topMetricsUsage;
            this.ttestUsage = ttestUsage;
        }

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
                boxplotUsage = in.readVLong();
            } else {
                boxplotUsage = 0;
            }
            cumulativeCardinalityUsage = in.readZLong();
            if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
                stringStatsUsage = in.readVLong();
                topMetricsUsage = in.readVLong();
            } else {
                stringStatsUsage = 0;
                topMetricsUsage = 0;
            }
            if (in.getVersion().onOrAfter(Version.V_7_8_0)) {
                ttestUsage = in.readVLong();
            } else {
                ttestUsage = 0;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
                out.writeVLong(boxplotUsage);
            }
            out.writeVLong(cumulativeCardinalityUsage);
            if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
                out.writeVLong(stringStatsUsage);
                out.writeVLong(topMetricsUsage);
            }
            if (out.getVersion().onOrAfter(Version.V_7_8_0)) {
                out.writeVLong(ttestUsage);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(BOXPLOT_USAGE.getPreferredName(), boxplotUsage);
            builder.field(CUMULATIVE_CARDINALITY_USAGE.getPreferredName(), cumulativeCardinalityUsage);
            builder.field(STRING_STATS_USAGE.getPreferredName(), stringStatsUsage);
            builder.field(TOP_METRICS_USAGE.getPreferredName(), topMetricsUsage);
            builder.field(T_TEST_USAGE.getPreferredName(), ttestUsage);
            builder.endObject();
            return builder;
        }

        public long getBoxplotUsage() {
            return boxplotUsage;
        }

        public long getCumulativeCardinalityUsage() {
            return cumulativeCardinalityUsage;
        }

        public long getStringStatsUsage() {
            return stringStatsUsage;
        }

        public long getTopMetricsUsage() {
            return topMetricsUsage;
        }

        public long getTTestUsage() {
            return topMetricsUsage;
        }
    }
}
