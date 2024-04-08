/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.analytics.action;

import org.elasticsearch.TransportVersions;
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
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.common.stats.EnumCounters;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

public class AnalyticsStatsAction extends ActionType<AnalyticsStatsAction.Response> {
    public static final AnalyticsStatsAction INSTANCE = new AnalyticsStatsAction();
    public static final String NAME = "cluster:monitor/xpack/analytics/stats";

    private AnalyticsStatsAction() {
        super(NAME);
    }

    /**
     * Items to track. Serialized by ordinals. Append only, don't remove or change order of items in this list.
     */
    public enum Item {
        BOXPLOT,
        CUMULATIVE_CARDINALITY,
        STRING_STATS,
        TOP_METRICS,
        T_TEST,
        MOVING_PERCENTILES,
        NORMALIZE,
        RATE,
        MULTI_TERMS;
    }

    public static class Request extends BaseNodesRequest<Request> implements ToXContentObject {

        public Request() {
            super((String[]) null);
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

        public NodeRequest() {}
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
            return in.readCollectionAsList(NodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            out.writeCollection(nodes);
        }

        public EnumCounters<Item> getStats() {
            List<EnumCounters<Item>> countersPerNode = getNodes().stream()
                .map(AnalyticsStatsAction.NodeResponse::getStats)
                .collect(Collectors.toList());
            return EnumCounters.merge(Item.class, countersPerNode);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            EnumCounters<Item> stats = getStats();
            builder.startObject("stats");
            for (Item item : Item.values()) {
                builder.field(item.name().toLowerCase(Locale.ROOT) + "_usage", stats.get(item));
            }
            builder.endObject();
            return builder;
        }
    }

    public static class NodeResponse extends BaseNodeResponse {
        private final EnumCounters<Item> counters;

        public NodeResponse(DiscoveryNode node, EnumCounters<Item> counters) {
            super(node);
            this.counters = counters;
        }

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_8_0)) {
                counters = new EnumCounters<>(in, Item.class);
            } else {
                counters = new EnumCounters<>(Item.class);
                if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_7_0)) {
                    counters.inc(Item.BOXPLOT, in.readVLong());
                }
                counters.inc(Item.CUMULATIVE_CARDINALITY, in.readZLong());
                if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_7_0)) {
                    counters.inc(Item.STRING_STATS, in.readVLong());
                    counters.inc(Item.TOP_METRICS, in.readVLong());
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_8_0)) {
                counters.writeTo(out);
            } else {
                if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_7_0)) {
                    out.writeVLong(counters.get(Item.BOXPLOT));
                }
                out.writeZLong(counters.get(Item.CUMULATIVE_CARDINALITY));
                if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_7_0)) {
                    out.writeVLong(counters.get(Item.STRING_STATS));
                    out.writeVLong(counters.get(Item.TOP_METRICS));
                }
            }
        }

        public EnumCounters<Item> getStats() {
            return counters;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeResponse that = (NodeResponse) o;
            return counters.equals(that.counters) && getNode().equals(that.getNode());
        }

        @Override
        public int hashCode() {
            return Objects.hash(counters, getNode());
        }
    }
}
