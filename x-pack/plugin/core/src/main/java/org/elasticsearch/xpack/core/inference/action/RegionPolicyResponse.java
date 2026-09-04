/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicyDoc;

import java.io.IOException;
import java.util.Objects;

public final class RegionPolicyResponse extends ActionResponse implements ToXContentObject {

    private final RegionPolicyDoc regionPolicy;

    public RegionPolicyResponse(RegionPolicyDoc regionPolicy) {
        this.regionPolicy = Objects.requireNonNull(regionPolicy);
    }

    public RegionPolicyResponse(StreamInput in) throws IOException {
        this(new RegionPolicyDoc(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        regionPolicy.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        regionPolicy.toXContent(builder, params);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        RegionPolicyResponse response = (RegionPolicyResponse) o;
        return Objects.equals(regionPolicy, response.regionPolicy);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(regionPolicy);
    }

    public RegionPolicyDoc regionPolicy() {
        return regionPolicy;
    }
}
