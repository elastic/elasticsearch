/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip.direct;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.ingest.geoip.direct.DatabaseConfigurationMetadata.DATABASE;
import static org.elasticsearch.ingest.geoip.direct.DatabaseConfigurationMetadata.MODIFIED_DATE;
import static org.elasticsearch.ingest.geoip.direct.DatabaseConfigurationMetadata.MODIFIED_DATE_MILLIS;
import static org.elasticsearch.ingest.geoip.direct.DatabaseConfigurationMetadata.VERSION;
import static org.elasticsearch.ingest.geoip.direct.GetDatabaseConfigurationAction.Response;

public class GetDatabaseConfigurationAction extends ActionType<Response> {
    public static final GetDatabaseConfigurationAction INSTANCE = new GetDatabaseConfigurationAction();
    public static final String NAME = "cluster:admin/ingest/geoip/database/get";

    protected GetDatabaseConfigurationAction() {
        super(NAME);
    }

    public static class Request extends BaseNodesRequest {
        private final String[] databaseIds;

        public Request(String... databaseIds) {
            super((String[]) null);
            this.databaseIds = databaseIds;
        }

        public String[] getDatabaseIds() {
            return databaseIds;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(databaseIds);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Arrays.equals(databaseIds, other.databaseIds);
        }

    }

    public static class Response extends BaseNodesResponse<NodeResponse> implements ToXContentObject {

        private final List<DatabaseConfigurationMetadata> databases;

        public Response(
            List<DatabaseConfigurationMetadata> databases,
            ClusterName clusterName,
            List<NodeResponse> nodes,
            List<FailedNodeException> failures
        ) {
            super(clusterName, nodes, failures);
            this.databases = List.copyOf(databases); // defensive copy
        }

        protected Response(StreamInput in) throws IOException {
            super(in);
            this.databases = in.readCollectionAsList(DatabaseConfigurationMetadata::new);
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
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray("databases");
            for (DatabaseConfigurationMetadata item : databases) {
                DatabaseConfiguration database = item.database();
                builder.startObject();
                builder.field("id", database.id()); // serialize including the id -- this is get response serialization
                builder.field(VERSION.getPreferredName(), item.version());
                builder.timestampFieldsFromUnixEpochMillis(
                    MODIFIED_DATE_MILLIS.getPreferredName(),
                    MODIFIED_DATE.getPreferredName(),
                    item.modifiedDate()
                );
                builder.field(DATABASE.getPreferredName(), database);
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }
    }

    public static class NodeRequest extends TransportRequest {

        private final String[] databaseIds;

        public NodeRequest(String... databaseIds) {
            super();
            this.databaseIds = Objects.requireNonNull(databaseIds, "ids may not be null");
        }

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            databaseIds = in.readStringArray();
        }

        public String[] getDatabaseIds() {
            return this.databaseIds;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(databaseIds);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(databaseIds);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            NodeRequest other = (NodeRequest) obj;
            return Arrays.equals(databaseIds, other.databaseIds);
        }
    }

    public static class NodeResponse extends BaseNodeResponse {

        private final List<DatabaseConfigurationMetadata> databases;

        public NodeResponse(DiscoveryNode node, List<DatabaseConfigurationMetadata> databases) {
            super(node);
            this.databases = List.copyOf(databases); // defensive copy
        }

        public NodeResponse(StreamInput in) throws IOException {
            super(in);
            this.databases = in.readCollectionAsList(DatabaseConfigurationMetadata::new);
        }

        public List<DatabaseConfigurationMetadata> getDatabases() {
            return this.databases;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(databases);
        }

        @Override
        public int hashCode() {
            return Objects.hash(databases);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            NodeResponse other = (NodeResponse) obj;
            return databases.equals(other.databases);
        }
    }
}
