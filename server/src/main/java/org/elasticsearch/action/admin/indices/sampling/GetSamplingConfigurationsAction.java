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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Action type for retrieving sampling configurations.
 * <p>
 * This action allows clients to get the current sampling configurations
 * that have been set on one or more indices. The response contains a mapping
 * from index names to their corresponding {@link SamplingConfiguration} objects.
 * </p>
 *
 * @see SamplingConfiguration
 * @see PutSampleConfigurationAction
 */
public class GetSamplingConfigurationsAction extends ActionType<GetSamplingConfigurationsAction.Response> {

    /**
     * Singleton instance of the GetSamplingConfigurationsAction.
     * This provides a shared reference to the action type throughout the application.
     */
    public static final GetSamplingConfigurationsAction INSTANCE = new GetSamplingConfigurationsAction();

    /**
     * The name identifier for this action type used in the transport layer.
     * Format follows the pattern: "indices:admin/sampling/config/get"
     */
    public static final String NAME = "indices:admin/sampling/config/getAll";

    private GetSamplingConfigurationsAction() {
        super(NAME);
    }

    /**
     * Response object containing a map from index names to their sampling configurations.
     * <p>
     * This response encapsulates a map where keys are index names and values are
     * their corresponding sampling configurations. If an index has no sampling
     * configuration, it will not be present in the map.
     * </p>
     */
    public static class Response extends ActionResponse implements ToXContentObject {
        private final Map<String, SamplingConfiguration> indexToSamplingConfigMap;

        /**
         * Constructs a new Response with the given index-to-configuration mapping.
         *
         * @param indexToSamplingConfigMap a map from index names to their sampling configurations.
         */
        public Response(Map<String, SamplingConfiguration> indexToSamplingConfigMap) {
            this.indexToSamplingConfigMap = indexToSamplingConfigMap;
        }

        /**
         * Constructs a new Response by deserializing from a StreamInput.
         * <p>
         * This constructor is used during deserialization when the response is received
         * over the transport layer. It reads the map of index names to sampling configurations
         * from the input stream.
         * </p>
         *
         * @param in the stream input to read from
         * @throws IOException if an I/O error occurs during deserialization
         */
        public Response(StreamInput in) throws IOException {
            this.indexToSamplingConfigMap = in.readMap(StreamInput::readString, SamplingConfiguration::new);
        }

        /**
         * Returns the mapping of index names to their sampling configurations.
         * <p>
         * The returned map contains all indices that have sampling configurations.
         * Indices without sampling configurations will not be present in this map.
         * </p>
         *
         * @return the index-to-sampling-configuration map
         */
        public Map<String, SamplingConfiguration> getIndexToSamplingConfigMap() {
            return indexToSamplingConfigMap;
        }

        /**
         * Serializes this response to a StreamOutput.
         * <p>
         * This method writes the index-to-configuration map to the output stream
         * so it can be transmitted over the transport layer. Each index name is
         * written as a string, followed by its corresponding sampling configuration.
         * </p>
         *
         * @param out the stream output to write to
         * @throws IOException if an I/O error occurs during serialization
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(indexToSamplingConfigMap, StreamOutput::writeString, (stream, config) -> config.writeTo(stream));
        }

        /**
         * Determines whether this response is equal to another object.
         * <p>
         * Two Response objects are considered equal if they contain the same
         * index-to-sampling-configuration mappings.
         * </p>
         *
         * @param o the object to compare with this response
         * @return true if the objects are equal, false otherwise
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return Objects.equals(indexToSamplingConfigMap, that.indexToSamplingConfigMap);
        }

        /**
         * Returns the hash code for this response.
         * <p>
         * The hash code is computed based on the index-to-sampling-configuration map,
         * ensuring that equal responses have the same hash code.
         * </p>
         *
         * @return the hash code value for this response
         */
        @Override
        public int hashCode() {
            return Objects.hash(indexToSamplingConfigMap);
        }

        /**
         * Converts this response to XContent format.
         * <p>
         * This method serializes the response into a format suitable for
         * XContent builders, allowing it to be easily included in
         * Elasticsearch responses.
         * </p>
         *
         * @param builder the XContent builder to use for serialization
         * @param params  additional parameters for serialization, if any
         * @return the XContent builder with the serialized response
         * @throws IOException if an I/O error occurs during serialization
         */
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("indices");
                builder.startArray();
                {
                    for (Map.Entry<String, SamplingConfiguration> entry : indexToSamplingConfigMap.entrySet()) {
                        builder.startObject();
                        {
                            builder.field("index", entry.getKey());
                            builder.field("sampling_configuration", entry.getValue());
                        }
                        builder.endObject();
                    }
                }
                builder.endArray();
            }
            builder.endObject();
            return builder;
        }
    }
}
