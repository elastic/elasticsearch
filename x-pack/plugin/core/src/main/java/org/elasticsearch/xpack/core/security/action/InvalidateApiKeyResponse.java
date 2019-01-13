/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authc.support.ApiKeysInvalidationResult;

import java.io.IOException;
import java.util.Objects;

/**
 * Response for invalidation of one or more API keys
 */
public final class InvalidateApiKeyResponse extends ActionResponse implements ToXContent {
    private ApiKeysInvalidationResult result;

    public InvalidateApiKeyResponse() {}

    public InvalidateApiKeyResponse(ApiKeysInvalidationResult result) {
        this.result = result;
    }

    public ApiKeysInvalidationResult getResult() {
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        result.writeTo(out);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        result = new ApiKeysInvalidationResult(in);
    }

    @Override
    public int hashCode() {
        return Objects.hash(result);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        InvalidateApiKeyResponse other = (InvalidateApiKeyResponse) obj;
        return Objects.equals(result, other.getResult());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        result.toXContent(builder, params);
        return builder;
    }

    @Override
    public String toString() {
        return "InvalidateApiKeyResponse [result=" + result + "]";
    }

}
