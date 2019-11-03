/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.io.IOException;

public class AuthenticateResponse extends ActionResponse {

    private Authentication authentication;

    public AuthenticateResponse(StreamInput in) throws IOException {
        super(in);
        authentication = new Authentication(in);
    }

    public AuthenticateResponse(Authentication authentication){
        this.authentication = authentication;
    }

    public Authentication authentication() {
        return authentication;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        authentication.writeTo(out);
    }

    }
