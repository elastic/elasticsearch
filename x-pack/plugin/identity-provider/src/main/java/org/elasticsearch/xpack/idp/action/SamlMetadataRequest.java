/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class SamlMetadataRequest extends ActionRequest {

    private String spEntityId;
    private String assertionConsumerService;

    public SamlMetadataRequest(StreamInput in) throws IOException {
        super(in);
        spEntityId = in.readString();
        assertionConsumerService = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(spEntityId);
        out.writeOptionalString(assertionConsumerService);
    }

    public SamlMetadataRequest(String spEntityId, @Nullable String acs) {
        this.spEntityId = Objects.requireNonNull(spEntityId, "Service Provider entity id must be provided");
        this.assertionConsumerService = acs;
    }

    public SamlMetadataRequest() {
        this.spEntityId = null;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getSpEntityId() {
        return spEntityId;
    }

    public void setSpEntityId(String spEntityId) {
        this.spEntityId = spEntityId;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{spEntityId='" + spEntityId + "' acs='" + assertionConsumerService + "'}";
    }

    public String getAssertionConsumerService() {
        return assertionConsumerService;
    }

    public void setAssertionConsumerService(String assertionConsumerService) {
        this.assertionConsumerService = assertionConsumerService;
    }
}
