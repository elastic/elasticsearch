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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * Action for configuring sampling settings on indices.
 * <p>
 * This action allows administrators to configure sampling parameters for one or more indices,
 * including sampling rate, maximum number of samples, maximum size constraints, time-to-live
 * settings, and conditional sampling criteria.
 * </p>
 * <p>
 * The action name is "indices:admin/sample/config/update" and it returns an {@link AcknowledgedResponse}
 * to indicate whether the configuration was successfully applied.
 * </p>
 */
public class PutSamplingConfigurationAction extends ActionType<AcknowledgedResponse> {
    /**
     * The action name used to identify this action in the transport layer.
     */
    public static final String NAME = "indices:admin/sample/config/update";

    /**
     * Singleton instance of this action type.
     */
    public static final PutSamplingConfigurationAction INSTANCE = new PutSamplingConfigurationAction();

    /**
     * Constructs a new PutSamplingConfigurationAction with the predefined action name.
     */
    public PutSamplingConfigurationAction() {
        super(NAME);
    }

    /**
     * Request class for configuring sampling settings on indices.
     * <p>
     * This request encapsulates all the parameters needed to configure sampling on one or more indices,
     * including the sampling configuration itself and the target indices. It implements
     * {@link IndicesRequest.Replaceable} to support index name resolution and expansion.
     * </p>
     */
    public static class Request extends AcknowledgedRequest<PutSamplingConfigurationAction.Request> implements IndicesRequest.Replaceable {
        private final SamplingConfiguration samplingConfiguration;
        private String[] indices = Strings.EMPTY_ARRAY;

        /**
         * Constructs a new request with the specified sampling configuration parameters.
         *
         * @param rate the sampling rate as a double between 0.0 and 1.0
         * @param maxSamples the maximum number of samples to collect, or null for the default
         * @param maxSize the maximum size of samples to collect, or null for the default
         * @param timeToLive the time-to-live for sampling data, or null for the default
         * @param condition the conditional expression for sampling, or null for unconditional sampling
         * @param masterNodeTimeout the timeout for master node operations, or null for default
         * @param ackTimeout the timeout for acknowledgment, or null for default
         */
        public Request(
            double rate,
            @Nullable Integer maxSamples,
            @Nullable ByteSizeValue maxSize,
            @Nullable TimeValue timeToLive,
            @Nullable String condition,
            @Nullable TimeValue masterNodeTimeout,
            @Nullable TimeValue ackTimeout
        ) {
            super(masterNodeTimeout, ackTimeout);
            this.samplingConfiguration = new SamplingConfiguration(rate, maxSamples, maxSize, timeToLive, condition);
        }

        /**
         * Constructs a new request by deserializing from a stream input.
         *
         * @param in the stream input to read from
         * @throws IOException if an I/O error occurs during deserialization
         */
        public Request(StreamInput in) throws IOException {
            super(in);
            this.indices = in.readStringArray();
            this.samplingConfiguration = new SamplingConfiguration(in);
        }

        /**
         * Serializes this request to a stream output.
         *
         * @param out the stream output to write to
         * @throws IOException if an I/O error occurs during serialization
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
            samplingConfiguration.writeTo(out);
        }

        /**
         * Returns the array of target indices for this sampling configuration request.
         *
         * @return an array of index names, never null but may be empty
         */
        @Override
        public String[] indices() {
            return indices;
        }

        /**
         * Sets the target indices for this sampling configuration request.
         *
         * @param dataStreamNames the names of indices or data streams to target
         * @return this request instance for method chaining
         */
        @Override
        public Request indices(String... dataStreamNames) {
            this.indices = dataStreamNames;
            return this;
        }

        /**
         * Indicates whether this request should include data streams in addition to regular indices.
         *
         * @return true to include data streams
         */
        @Override
        public boolean includeDataStreams() {
            return true;
        }

        /**
         * Returns the indices options for this request, which control how index names are resolved and expanded.
         *
         * @return the indices options, configured to be lenient and expand both open and closed indices
         */
        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED;
        }

        /**
         * Creates a cancellable task for tracking the execution of this request.
         *
         * @param id the unique task identifier
         * @param type the task type
         * @param action the action name
         * @param parentTaskId the parent task identifier, or null if this is a root task
         * @param headers the request headers
         * @return a new cancellable task instance
         */
        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        /**
         * Returns the sampling configuration encapsulated by this request.
         *
         * @return the sampling configuration
         */
        public SamplingConfiguration getSampleConfiguration() {
            return samplingConfiguration;
        }

        /**
         * Compares this request with another object for equality.
         * <p>
         * Two requests are considered equal if they have the same target indices and
         * sampling configuration parameters.
         * </p>
         *
         * @param o the object to compare with
         * @return true if the objects are equal, false otherwise
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request that = (Request) o;
            return Arrays.equals(indices, that.indices)
                && Objects.equals(samplingConfiguration, that.samplingConfiguration)
                && Objects.equals(masterNodeTimeout(), that.masterNodeTimeout())
                && Objects.equals(ackTimeout(), that.ackTimeout());
        }

        /**
         * Returns the hash code for this request.
         * <p>
         * The hash code is computed based on the target indices, sampling configuration,
         * master node timeout, and acknowledgment timeout.
         * </p>
         *
         * @return the hash code value
         */
        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(indices), samplingConfiguration, masterNodeTimeout(), ackTimeout());
        }
    }
}
