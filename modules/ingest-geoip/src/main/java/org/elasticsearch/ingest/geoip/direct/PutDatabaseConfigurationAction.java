/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.direct;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class PutDatabaseConfigurationAction extends ActionType<AcknowledgedResponse> {
    public static final PutDatabaseConfigurationAction INSTANCE = new PutDatabaseConfigurationAction();
    public static final String NAME = "cluster:admin/ingest/geoip/database/put";

    protected PutDatabaseConfigurationAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final DatabaseConfiguration database;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, DatabaseConfiguration database) {
            super(masterNodeTimeout, ackTimeout);
            this.database = database;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            database = new DatabaseConfiguration(in);
        }

        public DatabaseConfiguration getDatabase() {
            return this.database;
        }

        public static Request parseRequest(TimeValue masterNodeTimeout, TimeValue ackTimeout, String id, XContentParser parser) {
            return new Request(masterNodeTimeout, ackTimeout, DatabaseConfiguration.parse(parser, id));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            database.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return database.validate();
        }

        @Override
        public int hashCode() {
            return Objects.hash(database);
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
            return database.equals(other.database);
        }

        @Override
        public String toString() {
            return Strings.toString((b, p) -> b.field(database.id(), database));
        }
    }
}
