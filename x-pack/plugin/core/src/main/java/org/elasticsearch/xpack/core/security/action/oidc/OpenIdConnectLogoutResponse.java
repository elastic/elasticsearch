/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.oidc;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public final class OpenIdConnectLogoutResponse extends ActionResponse {

    private String endSessionUrl;

    public OpenIdConnectLogoutResponse(StreamInput in) throws IOException {
        super(in);
        this.endSessionUrl = in.readString();
    }

    public OpenIdConnectLogoutResponse(String endSessionUrl) {
        this.endSessionUrl = endSessionUrl;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(endSessionUrl);
    }

    public String toString() {
        return "{endSessionUrl=" + endSessionUrl + "}";
    }

    public String getEndSessionUrl() {
        return endSessionUrl;
    }
}
