/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.spatial.action;

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

public class SpatialStatsAction extends ActionType<SpatialStatsAction.Response> {
    public static final SpatialStatsAction INSTANCE = new SpatialStatsAction();
    public static final String NAME = "cluster:monitor/xpack/spatial/stats";

    private SpatialStatsAction() {
        super(NAME, Response::new);
    }

    /**
     * Items to track. Serialized by ordinals. Append only, don't remove or change order of items in this list.
     */
    public enum Item {
        GEOLINE,
        GEOHEX,
        CARTESIANCENTROID,
        CARTESIANBOUNDS
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

        public EnumCounters<Item> getStats() {
            List<EnumCounters<Item>> countersPerNode = getNodes().stream()
                .map(SpatialStatsAction.NodeResponse::getStats)
                .collect(Collectors.toList());
            return EnumCounters.merge(Item.class, countersPerNode);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            EnumCounters<Item> stats = getStats();
            builder.startObject();
            {
                builder.startObject("stats");
                {
                    for (Item item : Item.values()) {
                        builder.field(item.name().toLowerCase(Locale.ROOT) + "_usage", stats.get(item));
                    }
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(getStats());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response other = (Response) o;
            return Objects.equals(getStats(), other.getStats());
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
            counters = new EnumCounters<>(in, Item.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            counters.writeTo(out);
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
