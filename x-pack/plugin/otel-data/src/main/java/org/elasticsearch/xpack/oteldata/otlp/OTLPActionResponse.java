/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import com.google.protobuf.MessageLite;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class OTLPActionResponse extends ActionResponse {
    private final BytesReference response;
    private final RestStatus status;

    public OTLPActionResponse(RestStatus status, MessageLite response) {
        this(status, new BytesArray(response.toByteArray()));
    }

    public OTLPActionResponse(RestStatus status, BytesReference response) {
        this.response = response;
        this.status = status;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesReference(response);
        out.writeEnum(status);
    }

    public BytesReference getResponse() {
        return response;
    }

    public RestStatus getStatus() {
        return status;
    }
}
