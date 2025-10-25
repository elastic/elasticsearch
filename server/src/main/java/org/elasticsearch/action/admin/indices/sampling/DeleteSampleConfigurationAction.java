/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

/**
 * Action for deleting sampling configurations from indices.
 * <p>
 * This action allows administrators to remove sampling configurations from an index
 * or data stream. When a sampling configuration is deleted, any associated in-memory storage
 * and sampling state is cleaned up.
 * </p>
 * <p>
 * The action name is "indices:admin/sample/config/delete" and it returns an {@link AcknowledgedResponse}
 * to indicate whether the configuration was successfully removed.
 * </p>
 * Usage examples:
 * <ul>
 *   <li>{@code DELETE /logs/_sample/config} - Remove sampling configuration from the "logs" index</li>
 * </ul>
 *
 * @see SamplingConfiguration
 * @see PutSampleConfigurationAction
 */
public class DeleteSampleConfigurationAction extends ActionType<AcknowledgedResponse> {

    /**
     * The action name used to identify this action in the transport layer.
     */
    public static final String NAME = "indices:admin/sample/config/delete";

    /**
     * Singleton instance of this action type.
     */
    public static final DeleteSampleConfigurationAction INSTANCE = new DeleteSampleConfigurationAction();

    /**
     * Private constructor to enforce singleton pattern.
     * Creates a new action type with the predefined NAME.
     */
    private DeleteSampleConfigurationAction() {
        super(NAME);
    }

    /**
     * Request class for deleting sampling configurations from indices.
     * <p>
     * This request specifies which index or data stream should have their sampling
     * configurations removed. It extends {@link AcknowledgedRequest} to support
     * master node timeout and acknowledgment timeout settings.
     * </p>
     */
    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest.Replaceable {

        private String index;

        /**
         * Constructs a new request with specified timeouts.
         *
         * @param masterNodeTimeout the timeout for master node operations, or null for default
         * @param ackTimeout the timeout for acknowledgment, or null for default
         */
        public Request(@Nullable TimeValue masterNodeTimeout, @Nullable TimeValue ackTimeout) {
            super(masterNodeTimeout, ackTimeout);
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
         * Serializes this request to a StreamOutput.
         * <p>
         * This method is called during transport layer serialization when sending
         * the request to remote nodes.
         * </p>
         *
         * @param out the stream output to write to, must not be null
         * @throws IOException if an I/O error occurs during serialization
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
        }

        /**
         * Returns the array of index names or patterns targeted by this request.
         * <p>
         * The returned array contains the names of indices and/or data streams from which
         * sampling configurations should be deleted. Index patterns using wildcards
         * are supported according to the configured {@link #indicesOptions()}.
         * </p>
         *
         * @return an array of index names or patterns, never null but may be empty
         */
        @Override
        public String[] indices() {
            return new String[] { index };
        }

        /**
         * Sets the array of index name for this request.
         * <p>
         * Specifies which index and/or data stream should have their sampling
         * configurations deleted. Supports individual index names
         * </p>
         *
         * @param indices the index names or patterns to target, null will be converted to empty array
         * @return this request instance for method chaining
         */
        @Override
        public IndicesRequest indices(String... indices) {
            if (indices == null || indices.length != 1) {
                throw new IllegalArgumentException("[indices] must contain only one index");
            }
            this.index = indices[0];
            return this;
        }

        /**
         * Indicates whether this request should include data streams in addition to regular indices.
         *
         * @return true to include data streams, false to exclude them
         */
        @Override
        public boolean includeDataStreams() {
            return true;
        }

        /**
         * Returns the indices options that control how index name resolution is performed.
         * <p>
         * This configuration specifies how wildcards are expanded, whether closed indices
         * are included, and how missing indices are handled. The strict single index option
         * ensures proper validation and prevents accidental broad operations.
         * </p>
         *
         * @return the indices options for this request, never null
         * @see IndicesOptions#STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED_ALLOW_SELECTORS
         */
        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED_ALLOW_SELECTORS;
        }

        /**
         * Compares this request with another object for equality.
         * <p>
         * Two delete sampling configuration requests are considered equal if they target
         * the same indices and have the same timeout values.
         * </p>
         *
         * @param o the object to compare with, may be null
         * @return true if the objects are equal, false otherwise
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(index, request.index)
                && Objects.equals(this.masterNodeTimeout(), request.masterNodeTimeout())
                && Objects.equals(this.ackTimeout(), request.ackTimeout());
        }

        /**
         * Returns a hash code value for this request.
         * <p>
         * The hash code is computed based on the target indices and timeout values,
         * ensuring that equal requests produce the same hash code.
         * </p>
         *
         * @return a hash code value for this request
         */
        @Override
        public int hashCode() {
            return Objects.hash(index, masterNodeTimeout(), ackTimeout());
        }
    }
}
