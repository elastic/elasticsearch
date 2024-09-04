/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.direct;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
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

public class GetDatabaseConfigurationAction extends ActionType<GetDatabaseConfigurationAction.Response> {
    public static final GetDatabaseConfigurationAction INSTANCE = new GetDatabaseConfigurationAction();
    public static final String NAME = "cluster:admin/ingest/geoip/database/get";

    protected GetDatabaseConfigurationAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<GetDatabaseConfigurationAction.Request> {

        private final String[] databaseIds;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, String... databaseIds) {
            super(masterNodeTimeout, ackTimeout);
            this.databaseIds = Objects.requireNonNull(databaseIds, "ids may not be null");
        }

        public Request(StreamInput in) throws IOException {
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
            Request other = (Request) obj;
            return Arrays.equals(databaseIds, other.databaseIds);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<DatabaseConfigurationMetadata> databases;

        public Response(List<DatabaseConfigurationMetadata> databases) {
            this.databases = List.copyOf(databases); // defensive copy
        }

        public Response(StreamInput in) throws IOException {
            this(in.readCollectionAsList(DatabaseConfigurationMetadata::new));
        }

        public List<DatabaseConfigurationMetadata> getDatabases() {
            return this.databases;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
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
                builder.timeField(MODIFIED_DATE_MILLIS.getPreferredName(), MODIFIED_DATE.getPreferredName(), item.modifiedDate());
                builder.field(DATABASE.getPreferredName(), database);
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            return builder;
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
            Response other = (Response) obj;
            return databases.equals(other.databases);
        }
    }
}
