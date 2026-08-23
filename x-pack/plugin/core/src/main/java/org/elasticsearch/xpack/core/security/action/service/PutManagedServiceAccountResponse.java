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

public class PutManagedServiceAccountResponse extends ActionResponse implements ToXContentObject {

    public enum Result {
        CREATED,
        UPDATED
    }

    private final Result result;

    public PutManagedServiceAccountResponse(Result result) {
        this.result = result;
    }

    public PutManagedServiceAccountResponse(StreamInput in) throws IOException {
        result = in.readEnum(Result.class);
    }

    public Result getResult() {
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(result);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("created", result == Result.CREATED).endObject();
    }
}
