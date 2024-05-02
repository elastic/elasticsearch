/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.apache.http.pool.PoolStats;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class GetInferenceStatsAction extends ActionType<GetInferenceStatsAction.Response> {

    public static final GetInferenceStatsAction INSTANCE = new GetInferenceStatsAction();
    public static final String NAME = "cluster:monitor/xpack/inference/stats/get";

    public GetInferenceStatsAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<GetInferenceStatsAction.Request> {

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
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

    public static class Response extends ActionResponse implements ToXContentObject {
        private static final String CONNECTION_POOL_STATS_FIELD_NAME = "connection_pool_stats";

        private final ConnectionPoolStats connectionPoolStats;

        public Response(PoolStats poolStats) {
            connectionPoolStats = ConnectionPoolStats.of(poolStats);
        }

        public Response(StreamInput in) throws IOException {
            super(in);

            connectionPoolStats = new ConnectionPoolStats(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            connectionPoolStats.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CONNECTION_POOL_STATS_FIELD_NAME, connectionPoolStats, params);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(connectionPoolStats, response.connectionPoolStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectionPoolStats);
        }

        private static class ConnectionPoolStats implements ToXContentObject, Writeable {
            private static final String AVAILABLE_CONNECTIONS = "available_connections";
            private static final String LEASED_CONNECTIONS = "leased_connections";
            private static final String MAX_CONNECTIONS = "max_connections";
            private static final String PENDING_CONNECTIONS = "pending_connections";

            public static ConnectionPoolStats of(PoolStats poolStats) {
                return new ConnectionPoolStats(poolStats.getAvailable(), poolStats.getLeased(), poolStats.getMax(), poolStats.getPending());
            }

            private final int availableConnections;
            private final int leasedConnections;
            private final int maxConnections;
            private final int pendingConnections;

            ConnectionPoolStats(int availableConnections, int leasedConnections, int maxConnections, int pendingConnections) {
                this.availableConnections = availableConnections;
                this.leasedConnections = leasedConnections;
                this.maxConnections = maxConnections;
                this.pendingConnections = pendingConnections;
            }

            ConnectionPoolStats(StreamInput in) throws IOException {
                this.availableConnections = in.readVInt();
                this.leasedConnections = in.readVInt();
                this.maxConnections = in.readVInt();
                this.pendingConnections = in.readVInt();
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(AVAILABLE_CONNECTIONS, availableConnections);
                builder.field(LEASED_CONNECTIONS, leasedConnections);
                builder.field(MAX_CONNECTIONS, maxConnections);
                builder.field(PENDING_CONNECTIONS, pendingConnections);
                builder.endObject();

                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeVInt(availableConnections);
                out.writeVInt(leasedConnections);
                out.writeVInt(maxConnections);
                out.writeVInt(pendingConnections);
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                ConnectionPoolStats that = (ConnectionPoolStats) o;
                return availableConnections == that.availableConnections
                    && leasedConnections == that.leasedConnections
                    && maxConnections == that.maxConnections
                    && pendingConnections == that.pendingConnections;
            }

            @Override
            public int hashCode() {
                return Objects.hash(availableConnections, leasedConnections, maxConnections, pendingConnections);
            }
        }
    }
}
