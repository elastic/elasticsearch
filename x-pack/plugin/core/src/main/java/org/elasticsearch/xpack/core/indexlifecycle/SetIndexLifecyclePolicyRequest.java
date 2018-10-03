/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class SetIndexLifecyclePolicyRequest extends AcknowledgedRequest<SetIndexLifecyclePolicyRequest>
    implements IndicesRequest.Replaceable {

    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();
    private String policy;

    public SetIndexLifecyclePolicyRequest() {
    }

    public SetIndexLifecyclePolicyRequest(String policy, String... indices) {
        if (indices == null) {
            throw new IllegalArgumentException("indices cannot be null");
        }
        if (policy == null) {
            throw new IllegalArgumentException("policy cannot be null");
        }
        this.indices = indices;
        this.policy = policy;
    }

    @Override
    public SetIndexLifecyclePolicyRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    public SetIndexLifecyclePolicyRequest policy(String policy) {
        this.policy = policy;
        return this;
    }

    public String policy() {
        return policy;
    }

    public void indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        policy = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeString(policy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices), indicesOptions, policy);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        SetIndexLifecyclePolicyRequest other = (SetIndexLifecyclePolicyRequest) obj;
        return Objects.deepEquals(indices, other.indices) &&
            Objects.equals(indicesOptions, other.indicesOptions) &&
            Objects.equals(policy, other.policy);
    }

}
