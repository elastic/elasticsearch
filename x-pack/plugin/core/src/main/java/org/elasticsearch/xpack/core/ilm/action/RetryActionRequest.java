/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class RetryActionRequest extends AcknowledgedRequest<RetryActionRequest> implements IndicesRequest.Replaceable {
    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();
    private boolean requireError = true;

    public RetryActionRequest(TimeValue masterNodeTimeout, TimeValue ackTimeout, String... indices) {
        super(masterNodeTimeout, ackTimeout);
        this.indices = indices;
    }

    public RetryActionRequest(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.RETRY_ILM_ASYNC_ACTION_REQUIRE_ERROR)
            || in.getTransportVersion().isPatchFrom(TransportVersions.V_9_0_0)
            || in.getTransportVersion().isPatchFrom(TransportVersions.RETRY_ILM_ASYNC_ACTION_REQUIRE_ERROR_8_19)
            || in.getTransportVersion().isPatchFrom(TransportVersions.V_8_18_0)) {
            this.requireError = in.readBoolean();
        }
    }

    @Override
    public RetryActionRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public RetryActionRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    public void requireError(boolean requireError) {
        this.requireError = requireError;
    }

    public boolean requireError() {
        return requireError;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.RETRY_ILM_ASYNC_ACTION_REQUIRE_ERROR)
            || out.getTransportVersion().isPatchFrom(TransportVersions.V_9_0_0)
            || out.getTransportVersion().isPatchFrom(TransportVersions.RETRY_ILM_ASYNC_ACTION_REQUIRE_ERROR_8_19)
            || out.getTransportVersion().isPatchFrom(TransportVersions.V_8_18_0)) {
            out.writeBoolean(requireError);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices), indicesOptions, requireError);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        RetryActionRequest other = (RetryActionRequest) obj;
        return Objects.deepEquals(indices, other.indices)
            && Objects.equals(indicesOptions, other.indicesOptions)
            && requireError == other.requireError;
    }

}
