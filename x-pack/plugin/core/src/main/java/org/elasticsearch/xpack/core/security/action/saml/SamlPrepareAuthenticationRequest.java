/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * Represents a request to prepare a SAML {@code &lt;AuthnRequest&gt;}.
 */
public final class SamlPrepareAuthenticationRequest extends ActionRequest {

    @Nullable
    private String realmName;

    @Nullable
    private String assertionConsumerServiceURL;

    @Nullable
    private String relayState;

    public SamlPrepareAuthenticationRequest(StreamInput in) throws IOException {
        super(in);
        realmName = in.readOptionalString();
        assertionConsumerServiceURL = in.readOptionalString();
        relayState = in.readOptionalString();
    }

    public SamlPrepareAuthenticationRequest() {}

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getRealmName() {
        return realmName;
    }

    public void setRealmName(String realmName) {
        this.realmName = realmName;
    }

    public String getAssertionConsumerServiceURL() {
        return assertionConsumerServiceURL;
    }

    public void setAssertionConsumerServiceURL(String assertionConsumerServiceURL) {
        this.assertionConsumerServiceURL = assertionConsumerServiceURL;
    }

    public String getRelayState() {
        return relayState;
    }

    public void setRelayState(String relayState) {
        this.relayState = relayState;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "{"
            + "realmName="
            + realmName
            + ", assertionConsumerServiceURL="
            + assertionConsumerServiceURL
            + ", relayState="
            + relayState
            + '}';
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(realmName);
        out.writeOptionalString(assertionConsumerServiceURL);
        out.writeOptionalString(relayState);
    }
}
