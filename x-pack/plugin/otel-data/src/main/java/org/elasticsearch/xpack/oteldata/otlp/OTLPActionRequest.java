/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class OTLPActionRequest extends ActionRequest implements CompositeIndicesRequest {
    private final BytesReference request;

    public OTLPActionRequest(StreamInput in) throws IOException {
        super(in);
        request = in.readBytesReference();
    }

    public OTLPActionRequest(BytesReference request) {
        this.request = request;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public BytesReference getRequest() {
        return request;
    }
}
