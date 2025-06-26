/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class AuthenticateRequest extends LegacyActionRequest {

    public static final AuthenticateRequest INSTANCE = new AuthenticateRequest();

    public AuthenticateRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().before(TransportVersions.V_8_5_0)) {
            // Older versions included the username as a field
            in.readString();
        }
    }

    private AuthenticateRequest() {}

    @Override
    public ActionRequestValidationException validate() {
        // we cannot apply our validation rules here as an authenticate request could be for an LDAP user that doesn't fit our restrictions
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().before(TransportVersions.V_8_5_0)) {
            throw new IllegalStateException("cannot send authenticate request to a node of earlier version");
        }
    }
}
