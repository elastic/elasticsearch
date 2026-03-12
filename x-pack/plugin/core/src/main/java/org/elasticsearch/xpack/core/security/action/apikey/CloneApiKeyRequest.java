/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.support.Validation;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request for cloning an existing API key. The source key is identified by its encoded credential
 * (Base64(id + ":" + secret)). The new key has the same role descriptors as the source, with a new
 * name, id, and optional expiration and metadata.
 */
public final class CloneApiKeyRequest extends LegacyActionRequest {

    private final String id;
    private SecureString apiKey;
    private String name;
    /**
     * Expiration for the new key.
     * Null = use source key's expiry;
     * {@link TimeValue#MINUS_ONE} = explicit no expiry;
     * otherwise use this duration.
     */
    @Nullable
    private TimeValue expiration;
    @Nullable
    private Map<String, Object> metadata;
    private WriteRequest.RefreshPolicy refreshPolicy;

    public CloneApiKeyRequest() {
        // we generate the API key id immediately so it's part of the request body and is audited
        this.id = UUIDs.base64UUID();
    }

    public CloneApiKeyRequest(StreamInput in) throws IOException {
        super(in);
        this.id = in.readString();
        this.apiKey = in.readSecureString();
        this.name = in.readString();
        this.expiration = in.readOptionalTimeValue();
        this.metadata = in.readGenericMap();
        this.refreshPolicy = WriteRequest.RefreshPolicy.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeSecureString(apiKey);
        out.writeString(name);
        out.writeOptionalTimeValue(expiration);
        out.writeGenericMap(metadata);
        refreshPolicy.writeTo(out);
    }

    public SecureString getApiKey() {
        return apiKey;
    }

    public void setApiKey(SecureString apiKey) {
        this.apiKey = apiKey;
    }

    /**
     * Parses the source API key credential into an {@link ApiKeyCredentials} instance.
     * The clone endpoint only supports REST API keys as the source.
     *
     * @return the parsed source API key credentials, or {@code null} if there is no API key
     * @throws IllegalArgumentException if the api key is null, empty, or not valid (e.g. malformed Base64 or missing colon)
     */
    public ApiKeyCredentials getSourceApiKey() {
        return ApiKeyCredentials.parse(apiKey, null, ApiKey.Type.REST);
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Nullable
    public TimeValue getExpiration() {
        return expiration;
    }

    public void setExpiration(@Nullable TimeValue expiration) {
        this.expiration = expiration;
    }

    @Nullable
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(@Nullable Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public void setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = Objects.requireNonNull(refreshPolicy, "refresh policy may not be null");
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (apiKey == null || apiKey.getChars().length == 0) {
            validationException = addValidationError("api key credential is required", validationException);
        }
        Validation.Error nameError = Validation.ApiKey.validateName(name);
        if (nameError != null) {
            validationException = addValidationError(nameError.toString(), validationException);
        }
        Validation.Error metadataError = Validation.ApiKey.validateMetadata(metadata);
        if (metadataError != null) {
            validationException = addValidationError(metadataError.toString(), validationException);
        }
        if (refreshPolicy == null) {
            validationException = addValidationError("refresh policy is required", validationException);
        }
        return validationException;
    }
}
