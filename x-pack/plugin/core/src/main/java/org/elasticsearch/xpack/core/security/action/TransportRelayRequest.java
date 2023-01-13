/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class TransportRelayRequest extends ActionRequest {

    private final String action;
    private final String payload;

    // TODO: fields for remote privileges and other headers

    public TransportRelayRequest(String action, String payload) {
        this.action = action;
        this.payload = payload;
    }

    public TransportRelayRequest(StreamInput in) throws IOException {
        super(in);
        this.action = in.readString();
        this.payload = in.readString();
    }

    public String getAction() {
        return action;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
