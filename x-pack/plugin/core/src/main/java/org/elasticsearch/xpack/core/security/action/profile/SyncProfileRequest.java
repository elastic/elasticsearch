/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.action.GrantApiKeyRequest;

import java.io.IOException;

public class SyncProfileRequest extends ActionRequest {

    private final GrantApiKeyRequest.Grant grant;

    public SyncProfileRequest() {
        this.grant = new GrantApiKeyRequest.Grant();
    }

    public SyncProfileRequest(GrantApiKeyRequest.Grant grant) {
        this.grant = grant;
    }

    public SyncProfileRequest(StreamInput in) throws IOException {
        super(in);
        this.grant = new GrantApiKeyRequest.Grant(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        grant.writeTo(out);
    }

    // TODO: refresh policy

    public GrantApiKeyRequest.Grant getGrant() {
        return grant;
    }

    @Override
    public ActionRequestValidationException validate() {
        // TODO:
        return null;
    }
}
