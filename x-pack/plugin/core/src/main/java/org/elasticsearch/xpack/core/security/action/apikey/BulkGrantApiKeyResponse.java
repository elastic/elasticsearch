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
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.xcontent.XContentUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Response for bulk grant of API keys. Successfully created keys (including the secret, returned once)
 * are listed under {@code created}. Per-key failures are listed under {@code errors}, keyed by the
 * pre-generated API key id.
 */
public final class BulkGrantApiKeyResponse extends ActionResponse implements ToXContentObject, Writeable {

    private final List<CreateApiKeyResponse> created;
    private final Map<String, Exception> errorDetails;

    public BulkGrantApiKeyResponse(final List<CreateApiKeyResponse> created, final Map<String, Exception> errorDetails) {
        this.created = created;
        this.errorDetails = errorDetails;
    }

    public BulkGrantApiKeyResponse(StreamInput in) throws IOException {
        this.created = in.readCollectionAsList(CreateApiKeyResponse::new);
        this.errorDetails = in.readMap(StreamInput::readException);
    }

    public List<CreateApiKeyResponse> getCreated() {
        return created;
    }

    public Map<String, Exception> getErrorDetails() {
        return errorDetails;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("created");
        for (CreateApiKeyResponse createdKey : created) {
            createdKey.toXContent(builder, params);
        }
        builder.endArray();
        XContentUtils.maybeAddErrorDetails(builder, errorDetails);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(created);
        out.writeMap(errorDetails, StreamOutput::writeException);
    }

    @Override
    public String toString() {
        return "BulkGrantApiKeyResponse{created=" + created + ", errorDetails=" + errorDetails + '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<CreateApiKeyResponse> created;
        private final Map<String, Exception> errorDetails;

        public Builder() {
            created = new ArrayList<>();
            errorDetails = new HashMap<>();
        }

        public Builder created(final CreateApiKeyResponse response) {
            created.add(response);
            return this;
        }

        public Builder error(final String id, final Exception ex) {
            errorDetails.put(id, ex);
            return this;
        }

        public Map<String, Exception> getErrorDetails() {
            return errorDetails;
        }

        public BulkGrantApiKeyResponse build() {
            return new BulkGrantApiKeyResponse(created, errorDetails);
        }
    }
}
