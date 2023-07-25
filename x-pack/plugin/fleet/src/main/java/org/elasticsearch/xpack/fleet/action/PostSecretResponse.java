/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class PostSecretResponse extends ActionResponse implements ToXContentObject {

    private final RestStatus status;
    private final String id;

    public PostSecretResponse(RestStatus status, String id) {
        this.status = Objects.requireNonNull(status);
        this.id = id;
    }

    public PostSecretResponse(StreamInput in) throws IOException {
        super(in);
        this.status = in.readEnum(RestStatus.class);
        this.id = in.readString();
    }

    public RestStatus status() {
        return status;
    }

    public String id() {
        return id;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(status);
        out.writeString(id);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("id", id);
        builder.field("status", status.getStatus());
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PostSecretResponse that = (PostSecretResponse) o;
        return status == that.status && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, id);
    }
}
