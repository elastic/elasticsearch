/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import java.io.IOException;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

/**
 * Represents a request to prepare a SAML {@code &lt;AuthnRequest&gt;}.
 */
public final class SamlPrepareAuthenticationRequest extends ActionRequest {

    @Nullable
    private String realmName;

    @Nullable
    private String assertionConsumerServiceURL;

    public SamlPrepareAuthenticationRequest() {
    }

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

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "realmName=" + realmName +
                ", assertionConsumerServiceURL=" + assertionConsumerServiceURL +
                '}';
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        realmName = in.readOptionalString();
        assertionConsumerServiceURL = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(realmName);
        out.writeOptionalString(assertionConsumerServiceURL);
    }
}
