/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.esql.EsqlDataSourceActionNames;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Tests the live connection for a data source configuration supplied in the request body.
 * The configuration is not required to exist in cluster state — this action is intended for
 * validating a new or modified configuration before saving it.
 *
 * <p>No per-request timeout parameter is exposed: enforcing a wall-clock deadline on a blocking
 * {@link org.elasticsearch.xpack.esql.datasources.spi.ConnectorFactory#open} call would require
 * interrupting a GENERIC-pool thread, which is connector-dependent and non-trivial. The connector's
 * own connect-timeout setting applies. A per-request timeout can be added in a follow-up.
 */
public class TestDataSourceConnectionAction extends ActionType<TestDataSourceConnectionAction.Response> {

    public static final TestDataSourceConnectionAction INSTANCE = new TestDataSourceConnectionAction();
    public static final String NAME = EsqlDataSourceActionNames.ESQL_TEST_DATA_SOURCE_CONNECTION_ACTION_NAME;

    private TestDataSourceConnectionAction() {
        super(NAME);
    }

    /** Request body: {@code {"type": "...", "settings": {...}}}. */
    public static class Request extends ActionRequest {
        private static final ParseField TYPE = new ParseField("type");
        private static final ParseField SETTINGS = new ParseField("settings");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "esql_test_data_source_connection",
            false,
            args -> new Request((String) args[0], args[1] != null ? (Map<String, Object>) args[1] : Map.of())
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), SETTINGS);
        }

        public static Request fromXContent(XContentParser parser) throws IOException {
            return PARSER.parse(parser, null);
        }

        private final String type;
        private final Map<String, Object> rawSettings;

        public Request(String type, Map<String, Object> rawSettings) {
            this.type = Objects.requireNonNull(type, "type cannot be null");
            this.rawSettings = Objects.requireNonNull(rawSettings, "rawSettings cannot be null");
        }

        public String type() {
            return type;
        }

        public Map<String, Object> rawSettings() {
            return rawSettings;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof Request request) {
                return type.equals(request.type) && rawSettings.equals(request.rawSettings);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, rawSettings);
        }
    }

    /**
     * Result of a connection test. Use {@link #success()}, {@link #failure(String)}, or
     * {@link #untestable()} to construct instances.
     *
     * <p>Wire format: {@code {"status": "success"|"failure"|"untestable"[, "error": "..."]}}
     */
    public static class Response extends ActionResponse implements ToXContentObject {
        private static final String STATUS_SUCCESS = "success";
        private static final String STATUS_FAILURE = "failure";
        private static final String STATUS_UNTESTABLE = "untestable";

        private final String status;
        @Nullable
        private final String error;

        /** Probe ran and the backend is reachable. */
        public static Response success() {
            return new Response(STATUS_SUCCESS, null);
        }

        /**
         * Probe ran but failed.
         *
         * @param error human-readable reason; must not be {@code null}
         */
        public static Response failure(String error) {
            return new Response(STATUS_FAILURE, Objects.requireNonNull(error, "error"));
        }

        /** Type is valid but has no connectivity probe. */
        public static Response untestable() {
            return new Response(STATUS_UNTESTABLE, null);
        }

        private Response(String status, @Nullable String error) {
            this.status = Objects.requireNonNull(status, "status");
            this.error = error;
        }

        public String status() {
            return status;
        }

        @Nullable
        public String error() {
            return error;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("status", status);
            if (error != null) {
                builder.field("error", error);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof Response response) {
                return status.equals(response.status) && Objects.equals(error, response.error);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(status, error);
        }
    }
}
