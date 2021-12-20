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

public class ActivateProfileRequest extends ActionRequest {

    // TODO: move grant to separate class
    private final GrantApiKeyRequest.Grant grant;

    public ActivateProfileRequest() {
        this.grant = new GrantApiKeyRequest.Grant();
    }

    public ActivateProfileRequest(GrantApiKeyRequest.Grant grant) {
        this.grant = grant;
    }

    public ActivateProfileRequest(StreamInput in) throws IOException {
        super(in);
        this.grant = new GrantApiKeyRequest.Grant(in);
    }

    public GrantApiKeyRequest.Grant getGrant() {
        return grant;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        grant.writeTo(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
