/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class SamlInitiateSingleSignOnResponse extends ActionResponse {

    private String redirectUrl;
    private String responseBody;
    private String spEntityId;

    public SamlInitiateSingleSignOnResponse(StreamInput in) throws IOException {
        super(in);
        this.redirectUrl = in.readString();
        this.responseBody = in.readString();
        this.spEntityId = in.readString();
    }

    public SamlInitiateSingleSignOnResponse(String redirectUrl, String responseBody, String spEntityId) {
        this.redirectUrl = redirectUrl;
        this.responseBody = responseBody;
        this.spEntityId = spEntityId;
    }

    public String getRedirectUrl() {
        return redirectUrl;
    }

    public String getResponseBody() {
        return responseBody;
    }

    public String getSpEntityId() {
        return spEntityId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(redirectUrl);
        out.writeString(responseBody);
        out.writeString(spEntityId);
    }
}
