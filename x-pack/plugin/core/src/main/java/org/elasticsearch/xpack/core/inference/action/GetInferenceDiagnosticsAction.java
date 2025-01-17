/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.apache.http.pool.PoolStats;
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
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class GetInferenceDiagnosticsAction extends ActionType<GetInferenceDiagnosticsAction.Response> {

    public static final GetInferenceDiagnosticsAction INSTANCE = new GetInferenceDiagnosticsAction();
    public static final String NAME = "cluster:monitor/xpack/inference/diagnostics/get";

    public GetInferenceDiagnosticsAction() {
        super(NAME);
    }

    public static class Request extends BaseNodesRequest {

        public Request() {
            super((String[]) null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }

        @Override
        public int hashCode() {
            // The class doesn't have any members at the moment so return the same hash code
            return Objects.hash(NAME);
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
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            for (var entry : getNodesMap().entrySet()) {
                NodeResponse response = entry.getValue();

                builder.startObject(entry.getKey());
                response.toXContent(builder, params);
                builder.endObject();
            }

            builder.endObject();
            return builder;
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readCollectionAsList(NodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            out.writeCollection(nodes);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return Objects.equals(getNodes(), that.getNodes()) && Objects.equals(failures(), that.failures());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getNodes(), failures());
        }
    }

    public static class NodeResponse extends BaseNodeResponse implements ToXContentFragment {
        static final String CONNECTION_POOL_STATS_FIELD_NAME = "connection_pool_stats";

        private final ConnectionPoolStats connectionPoolStats;

        public NodeResponse(DiscoveryNode node, PoolStats poolStats) {
            super(node);
            connectionPoolStats = ConnectionPoolStats.of(poolStats);
        }

        public NodeResponse(StreamInput in) throws IOException {
            super(in);

            connectionPoolStats = new ConnectionPoolStats(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            connectionPoolStats.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(CONNECTION_POOL_STATS_FIELD_NAME, connectionPoolStats, params);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeResponse response = (NodeResponse) o;
            return Objects.equals(connectionPoolStats, response.connectionPoolStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectionPoolStats);
        }

        ConnectionPoolStats getConnectionPoolStats() {
            return connectionPoolStats;
        }

        static class ConnectionPoolStats implements ToXContentObject, Writeable {
            static final String LEASED_CONNECTIONS = "leased_connections";
            static final String PENDING_CONNECTIONS = "pending_connections";
            static final String AVAILABLE_CONNECTIONS = "available_connections";
            static final String MAX_CONNECTIONS = "max_connections";

            static ConnectionPoolStats of(PoolStats poolStats) {
                return new ConnectionPoolStats(poolStats.getLeased(), poolStats.getPending(), poolStats.getAvailable(), poolStats.getMax());
            }

            private final int leasedConnections;
            private final int pendingConnections;
            private final int availableConnections;
            private final int maxConnections;

            ConnectionPoolStats(int leasedConnections, int pendingConnections, int availableConnections, int maxConnections) {
                this.leasedConnections = leasedConnections;
                this.pendingConnections = pendingConnections;
                this.availableConnections = availableConnections;
                this.maxConnections = maxConnections;
            }

            ConnectionPoolStats(StreamInput in) throws IOException {
                this.leasedConnections = in.readVInt();
                this.pendingConnections = in.readVInt();
                this.availableConnections = in.readVInt();
                this.maxConnections = in.readVInt();
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(LEASED_CONNECTIONS, leasedConnections);
                builder.field(PENDING_CONNECTIONS, pendingConnections);
                builder.field(AVAILABLE_CONNECTIONS, availableConnections);
                builder.field(MAX_CONNECTIONS, maxConnections);
                builder.endObject();

                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeVInt(leasedConnections);
                out.writeVInt(pendingConnections);
                out.writeVInt(availableConnections);
                out.writeVInt(maxConnections);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                ConnectionPoolStats that = (ConnectionPoolStats) o;
                return leasedConnections == that.leasedConnections
                    && pendingConnections == that.pendingConnections
                    && availableConnections == that.availableConnections
                    && maxConnections == that.maxConnections;
            }

            @Override
            public int hashCode() {
                return Objects.hash(leasedConnections, pendingConnections, availableConnections, maxConnections);
            }

            int getLeasedConnections() {
                return leasedConnections;
            }

            int getPendingConnections() {
                return pendingConnections;
            }

            int getAvailableConnections() {
                return availableConnections;
            }

            int getMaxConnections() {
                return maxConnections;
            }
        }
    }
}
