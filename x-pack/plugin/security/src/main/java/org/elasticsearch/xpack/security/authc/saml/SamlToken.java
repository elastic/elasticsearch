/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import java.util.List;

import org.apache.commons.codec.binary.Hex;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

/**
 * A very lightweight {@link AuthenticationToken} to hold SAML content.
 * Due to the nature of SAML, it is impossible to know the {@link #principal() principal} for the token
 * until it is parsed and validated, so this token always returns a placeholder value.
 * @see SamlRealm#authenticate
 */
public class SamlToken implements AuthenticationToken {
    private byte[] content;
    private final List<String> allowedSamlRequestIds;
    private final String authenticatingRealm;

    /**
     * @param content The content of the SAML message. This should be raw XML. In particular it should <strong>not</strong> be
     *                base64 encoded.
     * @param allowedSamlRequestIds The request Ids for the authentication requests this SAML response is allowed to be in response to.
     * @param authenticatingRealm The realm that should autenticate this SAML message.
     */
    public SamlToken(byte[] content, List<String> allowedSamlRequestIds, @Nullable String authenticatingRealm) {
        this.content = content;
        this.allowedSamlRequestIds = allowedSamlRequestIds;
        this.authenticatingRealm = authenticatingRealm;
    }

    @Override
    public String principal() {
        return "<unauthenticated-saml-user>";
    }

    @Override
    public Object credentials() {
        return content;
    }

    @Override
    public void clearCredentials() {
        content = null;
    }

    public byte[] getContent() {
        return content;
    }

    public List<String> getAllowedSamlRequestIds() {
        return allowedSamlRequestIds;
    }

    public String getAuthenticatingRealm() {
        return authenticatingRealm;
    }


    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" + Strings.cleanTruncate(Hex.encodeHexString(content), 128) + "...}";
    }
}
