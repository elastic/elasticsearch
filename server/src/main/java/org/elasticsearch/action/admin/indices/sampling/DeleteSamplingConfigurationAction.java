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
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Action for deleting sampling configurations from indices.
 * <p>
 * This action allows administrators to remove sampling configurations from one or more indices
 * or data streams. When a sampling configuration is deleted, any associated in-memory storage
 * and sampling state is cleaned up.
 * </p>
 * <p>
 * The action name is "indices:admin/sampling/config/delete" and it returns an {@link AcknowledgedResponse}
 * to indicate whether the configuration was successfully removed.
 * </p>
 * Usage examples:
 * <ul>
 *   <li>{@code DELETE /logs/_sample/config} - Remove sampling configuration from the "logs" index</li>
 *   <li>{@code DELETE /logs,metrics/_sample/config} - Remove sampling configurations from multiple indices</li>
 * </ul>
 *
 * @see SamplingConfiguration
 * @see PutSampleConfigurationAction
 * @see GetSamplingConfigurationAction
 */
public class DeleteSamplingConfigurationAction extends ActionType<AcknowledgedResponse> {

    /**
     * The action name used to identify this action in the transport layer.
     */
    public static final String NAME = "indices:admin/sampling/config/delete";

    /**
     * Singleton instance of this action type.
     */
    public static final DeleteSamplingConfigurationAction INSTANCE = new DeleteSamplingConfigurationAction();

    /**
     * Private constructor to enforce singleton pattern.
     * Creates a new action type with the predefined NAME.
     */
    private DeleteSamplingConfigurationAction() {
        super(NAME);
    }

    /**
     * Request class for deleting sampling configurations from indices.
     * <p>
     * This request specifies which indices or data streams should have their sampling
     * configurations removed. It extends {@link AcknowledgedRequest} to support
     * master node timeout and acknowledgment timeout settings.
     * </p>
     */
    public static class Request extends AcknowledgedRequest<DeleteSamplingConfigurationAction.Request>
        implements
            IndicesRequest.Replaceable {

        private String[] indices = Strings.EMPTY_ARRAY;

        /**
         * Constructs a new request with specified timeouts.
         *
         * @param masterNodeTimeout the timeout for master node operations, or null for default
         * @param ackTimeout the timeout for acknowledgment, or null for default
         * @param indices the names of indices or data streams from which to remove sampling configurations
         */
        public Request(@Nullable TimeValue masterNodeTimeout, @Nullable TimeValue ackTimeout, String... indices) {
            super(masterNodeTimeout, ackTimeout);
            this.indices = indices != null ? indices : Strings.EMPTY_ARRAY;
        }

        /**
         * Constructs a new request by deserializing from a StreamInput.
         *
         * @param in the stream input to read from
         * @throws IOException if an I/O error occurs during deserialization
         */
        public Request(StreamInput in) throws IOException {
            super(in);
            this.indices = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.indices = indices != null ? indices : Strings.EMPTY_ARRAY;
            return this;
        }

        @Override
        public boolean includeDataStreams() {
            return true;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (indices == null || indices.length == 0) {
                validationException = addValidationError("at least one index name is required", validationException);
            }
            for (String index : indices) {
                if (Strings.isNullOrEmpty(index)) {
                    validationException = addValidationError("index name cannot be null or empty", validationException);
                    break;
                }
            }
            return validationException;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "Deletes Sampling Configuration.", parentTaskId, headers);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(indices, request.indices)
                && Objects.equals(this.masterNodeTimeout(), request.masterNodeTimeout())
                && Objects.equals(this.ackTimeout(), request.ackTimeout());
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(indices), masterNodeTimeout(), ackTimeout());
        }
    }
}
