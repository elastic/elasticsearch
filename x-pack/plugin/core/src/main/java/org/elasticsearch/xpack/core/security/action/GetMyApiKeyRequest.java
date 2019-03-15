/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for retrieving information for API key(s) owned by the authenticated user.
 */
public final class GetMyApiKeyRequest extends ActionRequest {

    private final String apiKeyId;
    private final String apiKeyName;

    public GetMyApiKeyRequest() {
        this(null, null);
    }

    public GetMyApiKeyRequest(StreamInput in) throws IOException {
        super(in);
        apiKeyId = in.readOptionalString();
        apiKeyName = in.readOptionalString();
    }

    public GetMyApiKeyRequest(@Nullable String apiKeyId, @Nullable String apiKeyName) {
        this.apiKeyId = apiKeyId;
        this.apiKeyName = apiKeyName;
    }

    public String getApiKeyId() {
        return apiKeyId;
    }

    public String getApiKeyName() {
        return apiKeyName;
    }

    /**
     * Creates request for given api key id
     * @param apiKeyId api key id
     * @return {@link GetMyApiKeyRequest}
     */
    public static GetMyApiKeyRequest usingApiKeyId(String apiKeyId) {
        return new GetMyApiKeyRequest(apiKeyId, null);
    }

    /**
     * Creates request for given api key name
     * @param apiKeyName api key name
     * @return {@link GetMyApiKeyRequest}
     */
    public static GetMyApiKeyRequest usingApiKeyName(String apiKeyName) {
        return new GetMyApiKeyRequest(null, apiKeyName);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(apiKeyId);
        out.writeOptionalString(apiKeyName);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public String toString() {
        return "GetMyApiKeyRequest [apiKeyId=" + apiKeyId + ", apiKeyName=" + apiKeyName + "]";
    }

}
