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
import java.util.Map;

public class SamlValidateAuthnRequestResponse extends ActionResponse {

    private String spEntityId;
    private boolean forceAuthn;
    private Map<String, Object> additionalData;

    public SamlValidateAuthnRequestResponse(StreamInput in) throws IOException {
        super(in);
        this.spEntityId = in.readString();
        this.forceAuthn = in.readBoolean();
        this.additionalData = in.readMap();
    }

    public SamlValidateAuthnRequestResponse(String spEntityId, boolean forceAuthn, Map<String, Object> additionalData) {
        this.spEntityId = spEntityId;
        this.forceAuthn = forceAuthn;
        this.additionalData = additionalData;
    }

    public String getSpEntityId() {
        return spEntityId;
    }

    public boolean isForceAuthn() {
        return forceAuthn;
    }

    public Map<String, Object> getAdditionalData() {
        return additionalData;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(spEntityId);
        out.writeBoolean(forceAuthn);
        out.writeMap(additionalData);

    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{ spEntityId='" + getSpEntityId() + "',\n" +
            " forceAuthn='" + isForceAuthn() + "',\n" +
            " additionalData='" + getAdditionalData() + "' }";
    }
}
