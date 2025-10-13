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
public class PutSampleConfigurationAction extends ActionType<AcknowledgedResponse> {
    /**
     * The action name used to identify this action in the transport layer.
     */
    public static final String NAME = "indices:admin/sample/config/update";

    /**
     * Singleton instance of this action type.
     */
    public static final PutSampleConfigurationAction INSTANCE = new PutSampleConfigurationAction();

    /**
     * Constructs a new PutSampleConfigurationAction with the predefined action name.
     */
    public PutSampleConfigurationAction() {
        super(NAME);
    }

    /**
     * Request class for configuring sampling settings on indices.
     * <p>
     * This request encapsulates all the parameters needed to configure sampling on one or more indices,
     * including the sampling configuration itself and the target indices. It implements
     * {@link Replaceable} to support index name resolution and expansion.
     * </p>
     */
    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest.Replaceable {
        private final SamplingConfiguration samplingConfiguration;
        private String index;

        /**
         * Constructs a new request with the specified sampling configuration parameters.
         *
         * @param samplingConfiguration the sampling configuration to apply
         * @param masterNodeTimeout the timeout for master node operations, or null for default
         * @param ackTimeout the timeout for acknowledgment, or null for default
         */
        public Request(SamplingConfiguration samplingConfiguration, @Nullable TimeValue masterNodeTimeout, @Nullable TimeValue ackTimeout) {
            super(masterNodeTimeout, ackTimeout);
            Objects.requireNonNull(samplingConfiguration, "samplingConfiguration must not be null");
            this.samplingConfiguration = samplingConfiguration;
        }

        /**
         * Constructs a new request by deserializing from a stream input.
         *
         * @param in the stream input to read from
         * @throws IOException if an I/O error occurs during deserialization
         */
        public Request(StreamInput in) throws IOException {
            super(in);
            this.index = in.readString();
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
            out.writeString(index);
            samplingConfiguration.writeTo(out);
        }

        /**
         * Returns the array of target indices for this sampling configuration request.
         *
         * @return an array of index names, never null but may be empty
         */
        @Override
        public String[] indices() {
            return index == null ? null : new String[] { index };
        }

        /**
         * Sets the target indices or data streams for this sampling configuration request.
         *
         * @param indices the names of indices or data streams to target
         * @return this request instance for method chaining
         */
        @Override
        public Request indices(String... indices) {
            if (indices == null || indices.length != 1) {
                throw new IllegalArgumentException("[indices] must contain only one index");
            }
            this.index = indices[0];
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
         * @return the indices options, configured to be strict about single index, no expansion, forbid closed indices, and allow selectors
         */
        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED_ALLOW_SELECTORS;
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
            return Objects.equals(index, that.index)
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
            return Objects.hash(index, samplingConfiguration, masterNodeTimeout(), ackTimeout());
        }
    }
}
