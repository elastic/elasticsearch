/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Objects;

public class SamlGenerateMetadataRequest extends ActionRequest {

    private String spEntityId;

    public SamlGenerateMetadataRequest(StreamInput in) throws IOException {
        super(in);
        spEntityId = in.readString();
    }

    public SamlGenerateMetadataRequest(String spEntityId) {
        this.spEntityId = Objects.requireNonNull(spEntityId, "Service Provider entity id must be provided");
    }

    public SamlGenerateMetadataRequest() {
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
        return getClass().getSimpleName() + "{spEntityId='" + spEntityId + "'}";
    }

}
