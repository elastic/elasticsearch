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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class AnalyticsStatsAction extends ActionType<AnalyticsStatsAction.Response> {
    public static final AnalyticsStatsAction INSTANCE = new AnalyticsStatsAction();
    public static final String NAME = "cluster:monitor/xpack/analytics/stats";

    private AnalyticsStatsAction() {
        super(NAME, Response::new);
    }

    /**
     * Items to track.
     */
    public enum Item {
        BOXPLOT,
        CUMULATIVE_CARDINALITY,
        STRING_STATS,
        TOP_METRICS,
        T_TEST;

        public String statName() {
            return name().toLowerCase(Locale.ROOT);
        }
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

    public static class Response extends BaseNodesResponse<NodeResponse> implements Writeable {
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
    }

    public static class NodeResponse extends BaseNodeResponse {
        private final Counters counters;

        public NodeResponse(DiscoveryNode node, Counters counters) {
            super(node);
            this.counters = counters;
        }

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            if (in.getVersion().onOrAfter(Version.V_7_8_0)) {
                counters = new Counters(in);
            } else {
                counters = new Counters();
                if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
                    counters.inc(Item.BOXPLOT.statName(), in.readLong());
                } else {
                    counters.set(Item.BOXPLOT.statName());
                }
                counters.inc(Item.CUMULATIVE_CARDINALITY.statName(), in.readZLong());
                if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
                    counters.inc(Item.STRING_STATS.statName(), in.readVLong());
                    counters.inc(Item.TOP_METRICS.statName(), in.readVLong());
                } else {
                    counters.set(Item.STRING_STATS.statName());
                    counters.set(Item.TOP_METRICS.statName());
                }
                counters.set(Item.T_TEST.statName());
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_7_8_0)) {
                counters.writeTo(out);
            } else {
                if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
                    out.writeVLong(counters.get(Item.BOXPLOT.statName()));
                }
                out.writeZLong(counters.get(Item.CUMULATIVE_CARDINALITY.statName()));
                if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
                    out.writeVLong(counters.get(Item.STRING_STATS.statName()));
                    out.writeVLong(counters.get(Item.TOP_METRICS.statName()));
                }
            }
        }

        public Counters getStats() {
            return counters;
        }
    }
}
