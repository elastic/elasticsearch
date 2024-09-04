/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.direct;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

public class DeleteDatabaseConfigurationAction extends ActionType<AcknowledgedResponse> {
    public static final DeleteDatabaseConfigurationAction INSTANCE = new DeleteDatabaseConfigurationAction();
    public static final String NAME = "cluster:admin/ingest/geoip/database/delete";

    protected DeleteDatabaseConfigurationAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final String databaseId;

        public Request(StreamInput in) throws IOException {
            super(in);
            databaseId = in.readString();
        }

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, String databaseId) {
            super(masterNodeTimeout, ackTimeout);
            this.databaseId = Objects.requireNonNull(databaseId, "id may not be null");
        }

        public String getDatabaseId() {
            return this.databaseId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(databaseId);
        }

        @Override
        public int hashCode() {
            return databaseId.hashCode();
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
            return Objects.equals(databaseId, other.databaseId);
        }
    }
}
