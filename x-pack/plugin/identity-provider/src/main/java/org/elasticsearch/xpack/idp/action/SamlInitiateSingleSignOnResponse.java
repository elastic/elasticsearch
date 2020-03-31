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

    private String postUrl;
    private String samlResponse;
    private String entityId;

    public SamlInitiateSingleSignOnResponse(StreamInput in) throws IOException {
        super(in);
        this.postUrl = in.readString();
        this.samlResponse = in.readString();
        this.entityId = in.readString();
    }

    public SamlInitiateSingleSignOnResponse(String postUrl, String samlResponse, String entityId) {
        this.postUrl = postUrl;
        this.samlResponse = samlResponse;
        this.entityId = entityId;
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(postUrl);
        out.writeString(samlResponse);
        out.writeString(entityId);
    }
}
