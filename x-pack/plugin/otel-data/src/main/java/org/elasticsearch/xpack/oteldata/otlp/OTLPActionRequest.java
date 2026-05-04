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
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;

public class OTLPActionRequest extends ActionRequest implements CompositeIndicesRequest {

    private final BytesReference request;
    private final MappingMode requestMappingMode;

    public OTLPActionRequest(BytesReference request) {
        this(request, MappingMode.OTEL);
    }

    public OTLPActionRequest(BytesReference request, MappingMode requestMappingMode) {
        this.request = request;
        this.requestMappingMode = requestMappingMode;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) {
        // OTLP REST actions execute locally; the request body is not serialized over transport.
        TransportAction.localOnly();
    }

    public BytesReference getRequest() {
        return request;
    }

    public MappingMode getRequestMappingMode() {
        return requestMappingMode;
    }
}
