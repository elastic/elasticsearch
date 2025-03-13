/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class SamlInitiateSingleSignOnResponse extends ActionResponse {

    private final String postUrl;
    private final String samlResponse;
    private final String entityId;
    private final String samlStatus;
    private final String error;

    public SamlInitiateSingleSignOnResponse(
        String entityId,
        String postUrl,
        String samlResponse,
        String samlStatus,
        @Nullable String error
    ) {
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

    public void toXContent(XContentBuilder builder) throws IOException {
        builder.field("post_url", this.getPostUrl());
        builder.field("saml_response", this.getSamlResponse());
        builder.field("saml_status", this.getSamlStatus());
        builder.field("error", this.getError());
        builder.startObject("service_provider");
        builder.field("entity_id", this.getEntityId());
        builder.endObject();
    }
}
