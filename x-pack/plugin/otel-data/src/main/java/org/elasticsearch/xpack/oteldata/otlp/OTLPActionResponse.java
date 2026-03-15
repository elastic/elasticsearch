/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import com.google.protobuf.MessageLite;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;

public class OTLPActionResponse extends ActionResponse {
    private final BytesReference response;

    public OTLPActionResponse(MessageLite response) {
        this(new BytesArray(response.toByteArray()));
    }

    public OTLPActionResponse(BytesReference response) {
        this.response = response;
    }

    @Override
    public void writeTo(StreamOutput out) {
        TransportAction.localOnly();
    }

    public BytesReference getResponse() {
        return response;
    }
}
