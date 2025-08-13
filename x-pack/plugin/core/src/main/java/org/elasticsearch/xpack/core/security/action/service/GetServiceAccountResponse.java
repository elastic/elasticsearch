/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class GetServiceAccountResponse extends ActionResponse implements ToXContentObject {

    private final ServiceAccountInfo[] serviceAccountInfos;

    public GetServiceAccountResponse(ServiceAccountInfo[] serviceAccountInfos) {
        this.serviceAccountInfos = Objects.requireNonNull(serviceAccountInfos);
    }

    public GetServiceAccountResponse(StreamInput in) throws IOException {
        this.serviceAccountInfos = in.readArray(ServiceAccountInfo::new, ServiceAccountInfo[]::new);
    }

    public ServiceAccountInfo[] getServiceAccountInfos() {
        return serviceAccountInfos;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(serviceAccountInfos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (ServiceAccountInfo info : serviceAccountInfos) {
            info.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "GetServiceAccountResponse{" + "serviceAccountInfos=" + Arrays.toString(serviceAccountInfos) + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetServiceAccountResponse that = (GetServiceAccountResponse) o;
        return Arrays.equals(serviceAccountInfos, that.serviceAccountInfos);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(serviceAccountInfos);
    }
}
