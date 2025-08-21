/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class ConnectorCreateActionResponse extends ActionResponse implements ToXContentObject {

    private final String id;
    private final DocWriteResponse.Result result;

    public ConnectorCreateActionResponse(StreamInput in) throws IOException {
        this.id = in.readString();
        this.result = DocWriteResponse.Result.readFrom(in);
    }

    public ConnectorCreateActionResponse(String id, DocWriteResponse.Result result) {
        this.id = id;
        this.result = result;
    }

    public String getId() {
        return id;
    }

    public DocWriteResponse.Result getResult() {
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        result.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", this.id);
        builder.field("result", this.result.getLowercase());
        builder.endObject();
        return builder;
    }

    public RestStatus status() {
        return switch (result) {
            case CREATED -> RestStatus.CREATED;
            case NOT_FOUND -> RestStatus.NOT_FOUND;
            default -> RestStatus.OK;
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectorCreateActionResponse that = (ConnectorCreateActionResponse) o;
        return Objects.equals(id, that.id) && result == that.result;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, result);
    }
}
