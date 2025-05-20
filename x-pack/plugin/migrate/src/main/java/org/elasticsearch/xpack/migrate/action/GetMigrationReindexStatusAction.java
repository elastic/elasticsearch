/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.migrate.task.ReindexDataStreamEnrichedStatus;

import java.io.IOException;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class GetMigrationReindexStatusAction extends ActionType<GetMigrationReindexStatusAction.Response> {

    public static final GetMigrationReindexStatusAction INSTANCE = new GetMigrationReindexStatusAction();
    public static final String NAME = "indices:admin/migration/reindex_status";

    public GetMigrationReindexStatusAction() {
        super(NAME);
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private final ReindexDataStreamEnrichedStatus enrichedStatus;

        public Response(ReindexDataStreamEnrichedStatus enrichedStatus) {
            this.enrichedStatus = requireNonNull(enrichedStatus, "status is required");
        }

        public Response(StreamInput in) throws IOException {
            enrichedStatus = in.readOptionalWriteable(ReindexDataStreamEnrichedStatus::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(enrichedStatus);
        }

        /**
         * Get the actual result of the fetch.
         */
        public ReindexDataStreamEnrichedStatus getEnrichedStatus() {
            return enrichedStatus;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            enrichedStatus.toXContent(builder, params);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(enrichedStatus);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Response && enrichedStatus.equals(((Response) other).enrichedStatus);
        }

        @Override
        public String toString() {
            String toString = Strings.toString(this);
            return toString.isEmpty() ? "unavailable" : toString;
        }

    }

    public static class Request extends LegacyActionRequest implements IndicesRequest {
        private final String index;

        public Request(String index) {
            super();
            this.index = index;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.index = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String getIndex() {
            return index;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(index);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Request && index.equals(((Request) other).index);
        }

        public Request nodeRequest(String thisNodeId, long thisTaskId) {
            Request copy = new Request(index);
            copy.setParentTask(thisNodeId, thisTaskId);
            return copy;
        }

        @Override
        public String[] indices() {
            return new String[] { index };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }
    }
}
