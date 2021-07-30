/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.action.ApiKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Response for search API keys.<br>
 * The result contains information about the API keys that were found.
 */
public final class QueryApiKeyResponse extends ActionResponse implements ToXContentObject, Writeable {

    private final ApiKey[] foundApiKeysInfo;

    public QueryApiKeyResponse(StreamInput in) throws IOException {
        super(in);
        this.foundApiKeysInfo = in.readArray(ApiKey::new, ApiKey[]::new);
    }

    public QueryApiKeyResponse(Collection<ApiKey> foundApiKeysInfo) {
        Objects.requireNonNull(foundApiKeysInfo, "found_api_keys_info must be provided");
        this.foundApiKeysInfo = foundApiKeysInfo.toArray(new ApiKey[0]);
    }

    public static QueryApiKeyResponse emptyResponse() {
        return new QueryApiKeyResponse(Collections.emptyList());
    }

    public ApiKey[] getApiKeyInfos() {
        return foundApiKeysInfo;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .array("api_keys", (Object[]) foundApiKeysInfo);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(foundApiKeysInfo);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        QueryApiKeyResponse that = (QueryApiKeyResponse) o;
        return Arrays.equals(foundApiKeysInfo, that.foundApiKeysInfo);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(foundApiKeysInfo);
    }

    @Override
    public String toString() {
        return "QueryApiKeyResponse [foundApiKeysInfo=" + foundApiKeysInfo + "]";
    }

}
