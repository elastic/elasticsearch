/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Action type for retrieving sampling configuration for a specific index.
 * <p>
 * This action allows clients to get the current sampling configuration
 * that has been set on a specific index. This action
 * targets a single index and returns its configuration in a structured format.
 * </p>
 * The response format matches:
 *  <pre>
 * {
 *  "index": "logs",
 *  "configuration": {
 *      "rate": ".5",
 *      "if": "ctx?.network?.name == 'Guest'"
 *  }
 * }
 *  </pre>
 *
 * @see SamplingConfiguration
 * @see PutSampleConfigurationAction
 */
public class GetSampleConfigurationAction extends ActionType<GetSampleConfigurationAction.Response> {

    /**
     * Singleton instance of the GetSampleConfigurationAction.
     * This provides a shared reference to the action type throughout the application.
     */
    public static final GetSampleConfigurationAction INSTANCE = new GetSampleConfigurationAction();

    /**
     * The name identifier for this action type used in the transport layer.
     */
    public static final String NAME = "indices:monitor/sample/config/get";

    private GetSampleConfigurationAction() {
        super(NAME);
    }

    /**
     * Request object for getting the sampling configuration of a specific index.
     * <p>
     * This request specifies which index's sampling configuration should be retrieved.
     * The index name must be provided and cannot be null or empty.
     * </p>
     */
    public static class Request extends LocalClusterStateRequest implements IndicesRequest.Replaceable {
        private String index;

        /**
         * Constructs a new request for the specified index.
         *
         * @param masterNodeTimeout the timeout for master node operations, or null for default
         */
        public Request(@Nullable TimeValue masterNodeTimeout) {
            super(masterNodeTimeout);
        }

        /**
         * Constructs a new request by deserializing from a StreamInput.
         *
         * @param in the stream input to read from
         * @throws IOException if an I/O error occurs during deserialization
         */
        public Request(StreamInput in) throws IOException {
            super(in);
            this.index = in.readString();
        }

        /**
         * Gets the index name for which to retrieve the sampling configuration.
         *
         * @return the index name
         */
        public String getIndex() {
            return index;
        }

        @Override
        public String[] indices() {
            return new String[] { index };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED_ALLOW_SELECTORS;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.index = indices[0];
            return this;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.isNullOrEmpty(index)) {
                validationException = addValidationError("index name is required", validationException);
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(index, request.index);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "executing " + action, parentTaskId, headers);
        }
    }

    /**
     * Response object containing the sampling configuration for a specific index.
     * <p>
     * This response contains the index name and its associated sampling configuration.
     * The response is designed to match the expected JSON format with an array containing
     * a single object with "index" and "configuration" fields.
     * </p>
     */
    public static class Response extends ActionResponse implements ChunkedToXContentObject {
        private final String index;
        private final SamplingConfiguration configuration;

        /**
         * Constructs a new Response with the given index and configuration.
         *
         * @param index the index name
         * @param configuration the sampling configuration for the index, or null if no configuration exists
         */
        public Response(String index, SamplingConfiguration configuration) {
            this.index = index;
            this.configuration = configuration;
        }

        /**
         * Constructs a new Response by deserializing from a StreamInput.
         *
         * @param in the stream input to read from
         * @throws IOException if an I/O error occurs during deserialization
         */
        public Response(StreamInput in) throws IOException {
            this.index = in.readString();
            this.configuration = in.readOptionalWriteable(SamplingConfiguration::new);
        }

        /**
         * Gets the index name.
         *
         * @return the index name
         */
        public String getIndex() {
            return index;
        }

        /**
         * Gets the sampling configuration for the index.
         *
         * @return the sampling configuration, or null if no configuration exists for this index
         */
        public SamplingConfiguration getConfiguration() {
            return configuration;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeOptionalWriteable(configuration);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(index, response.index) && Objects.equals(configuration, response.configuration);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, configuration);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Collections.singletonList((ToXContent) (builder, p) -> toXContent(builder, params)).iterator();
        }

        private XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.field("index", index);
            if (configuration != null) {
                builder.field("configuration", configuration);
            } else {
                builder.nullField("configuration");
            }
            builder.endObject();
            return builder;
        }

    }
}
