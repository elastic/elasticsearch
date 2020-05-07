/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * The verification response to the LogoutResponse from idP
 */
public final class SamlVerifyLogoutResponse extends ActionResponse {

    public SamlVerifyLogoutResponse(StreamInput in) throws IOException {
        super(in);
    }

    public SamlVerifyLogoutResponse() {
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }
}
