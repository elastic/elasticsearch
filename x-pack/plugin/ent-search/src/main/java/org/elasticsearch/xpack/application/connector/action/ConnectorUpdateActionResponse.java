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
import org.elasticsearch.xpack.application.connector.Connector;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJob;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents a response for update actions related to {@link Connector} and {@link ConnectorSyncJob}.
 * The response encapsulates the result of the update action, represented by a {@link DocWriteResponse.Result}.
 */
public class ConnectorUpdateActionResponse extends ActionResponse implements ToXContentObject {
    final DocWriteResponse.Result result;

    public ConnectorUpdateActionResponse(StreamInput in) throws IOException {
        result = DocWriteResponse.Result.readFrom(in);
    }

    public ConnectorUpdateActionResponse(DocWriteResponse.Result result) {
        this.result = result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.result.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("result", this.result.getLowercase());
        builder.endObject();
        return builder;
    }

    public RestStatus status() {
        return switch (result) {
            case NOT_FOUND -> RestStatus.NOT_FOUND;
            default -> RestStatus.OK;
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectorUpdateActionResponse that = (ConnectorUpdateActionResponse) o;
        return Objects.equals(result, that.result);
    }

    @Override
    public int hashCode() {
        return Objects.hash(result);
    }
}
