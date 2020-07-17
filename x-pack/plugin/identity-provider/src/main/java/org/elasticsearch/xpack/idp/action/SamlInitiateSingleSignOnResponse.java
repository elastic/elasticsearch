/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class SamlInitiateSingleSignOnResponse extends ActionResponse {

    private String postUrl;
    private String samlResponse;
    private String entityId;
    private String samlStatus;
    private String error;

    public SamlInitiateSingleSignOnResponse(StreamInput in) throws IOException {
        super(in);
        this.entityId = in.readString();
        this.postUrl = in.readString();
        this.samlResponse = in.readString();
        this.samlStatus = in.readString();
        this.error = in.readOptionalString();
    }

    public SamlInitiateSingleSignOnResponse(String entityId, String postUrl, String samlResponse, String samlStatus,
                                            @Nullable String error) {
        this.entityId = entityId;
        this.postUrl = postUrl;
        this.samlResponse = samlResponse;
        this.samlStatus = samlStatus;
        this.error = error;
    }

    public String getPostUrl() {
        return postUrl;
    }

    public String getSamlResponse() {
        return samlResponse;
    }

    public String getEntityId() {
        return entityId;
    }

    public String getError() {
        return error;
    }

    public String getSamlStatus() {
        return samlStatus;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(entityId);
        out.writeString(postUrl);
        out.writeString(samlResponse);
        out.writeString(samlStatus);
        out.writeOptionalString(error);
    }
}
