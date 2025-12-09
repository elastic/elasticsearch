/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
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

/**
 * Action type for retrieving all sampling configurations for cluster indices.
 * <p>
 * This action allows clients to get all sampling configurations
 * that have been set. This action
 * targets all indices and returns any existing configurations in a structured format.
 * </p>
 * The response format matches:
 *  <pre>
 * [
 * {
 *  "index": "logs",
 *  "configuration": {
 *      "rate": ".5",
 *      "if": "ctx?.network?.name == 'Guest'"
 *  }
 * },
 * {
 *  "index": "logsTwo",
 *  "configuration": {
 *      "rate": ".75"
 *  }
 * },
 * ]
 *  </pre>
 *
 * @see SamplingConfiguration
 * @see PutSampleConfigurationAction
 */
public class GetAllSampleConfigurationAction extends ActionType<GetAllSampleConfigurationAction.Response> {

    /**
     * Singleton instance of the GetAllSampleConfigurationAction.
     * This provides a shared reference to the action type throughout the application.
     */
    public static final GetAllSampleConfigurationAction INSTANCE = new GetAllSampleConfigurationAction();

    /**
     * The name identifier for this action type used in the transport layer.
     */
    public static final String NAME = "indices:monitor/sample/config/get_all";

    private GetAllSampleConfigurationAction() {
        super(NAME);
    }

    /**
     * Request object for getting all sampling configurations in a cluster.
     */
    public static class Request extends LocalClusterStateRequest implements IndicesRequest.Replaceable {
        /**
         * Constructs a new request.
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
        }

        @Override
        public String[] indices() {
            // This action reads from cluster state metadata (SamplingMetadata), not from indices directly.
            // The IndicesRequest implementation is required for security authorization checks.
            return new String[] { "*" };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.LENIENT_EXPAND_OPEN;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        @Override
        public Request indices(String... indices) {
            // This is a get-all operation, so we don't support changing the indices
            return this;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "executing " + action, parentTaskId, headers);
        }
    }

    /**
     * Response object containing an index to sampling configuration map.
     * <p>
     * This response contains a map from index names to their associated sampling configurations.
     * The response is designed to match the expected JSON format with an array containing
     * objects with "index" and "configuration" fields.
     * </p>
     */
    public static class Response extends ActionResponse implements ChunkedToXContentObject {
        private final Map<String, SamplingConfiguration> indexToSamplingConfigMap;

        /**
         * Constructs a new Response with the given index to sampling configuration map.
         *
         * @param indexToSamplingConfigMap the index to sampling configuration map
         */
        public Response(Map<String, SamplingConfiguration> indexToSamplingConfigMap) {
            this.indexToSamplingConfigMap = indexToSamplingConfigMap;
        }

        /**
         * Constructs a new Response by deserializing from a StreamInput.
         *
         * @param in the stream input to read from
         * @throws IOException if an I/O error occurs during deserialization
         */
        public Response(StreamInput in) throws IOException {
            this.indexToSamplingConfigMap = in.readMap(StreamInput::readString, SamplingConfiguration::new);
        }

        /**
         * Gets the index to sampling configuration map.
         *
         * @return the index to sampling configuration map, or an empty map if none exist
         */
        public Map<String, SamplingConfiguration> getIndexToSamplingConfigMap() {
            return indexToSamplingConfigMap == null ? Map.of() : indexToSamplingConfigMap;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(indexToSamplingConfigMap, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return indexToSamplingConfigMap.equals(that.indexToSamplingConfigMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexToSamplingConfigMap);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Collections.singletonList((ToXContent) (builder, p) -> toXContent(builder, params)).iterator();
        }

        private XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject().startArray("configurations");
            for (Map.Entry<String, SamplingConfiguration> entry : indexToSamplingConfigMap.entrySet()) {
                builder.startObject();
                builder.field("index", entry.getKey());
                builder.field("configuration", entry.getValue());
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }
    }
}
