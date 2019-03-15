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
 * Request for invalidating API key(s) for the authenticated user so that it can no longer be used.
 */
public final class InvalidateMyApiKeyRequest extends ActionRequest {

    private final String id;
    private final String name;

    public InvalidateMyApiKeyRequest() {
        this(null, null);
    }

    public InvalidateMyApiKeyRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readOptionalString();
        name = in.readOptionalString();
    }

    public InvalidateMyApiKeyRequest(@Nullable String id, @Nullable String name) {
        this.id = id;
        this.name = name;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    /**
     * Creates invalidate API key request for given api key id
     * @param id api key id
     * @return {@link InvalidateMyApiKeyRequest}
     */
    public static InvalidateMyApiKeyRequest usingApiKeyId(String id) {
        return new InvalidateMyApiKeyRequest(id, null);
    }

    /**
     * Creates invalidate api key request for given api key name
     * @param name api key name
     * @return {@link InvalidateMyApiKeyRequest}
     */
    public static InvalidateMyApiKeyRequest usingApiKeyName(String name) {
        return new InvalidateMyApiKeyRequest(null, name);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(id);
        out.writeOptionalString(name);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public String toString() {
        return "InvalidateMyApiKeyRequest [id=" + id + ", name=" + name + "]";
    }

}
