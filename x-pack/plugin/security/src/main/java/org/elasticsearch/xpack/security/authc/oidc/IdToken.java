/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import org.elasticsearch.xpack.security.authc.support.jwt.JsonWebToken;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IdToken extends JsonWebToken {
    public IdToken(Map<String, Object> header, Map<String, Object> payload) {
        super(header, payload);
    }

    public IdToken(Map<String, Object> header, Map<String, Object> payload, String signature) {
        super(header, payload, signature);
    }

    public IdToken(JsonWebToken token) {
        super(token.getHeader(), token.getPayload());
    }

    /**
     * Returns the iss claim value of the ID Token as a String
     */
    public String getIssuer() {
        return (String) getPayload().get("iss");
    }

    /**
     * Returns the sub claim value of the ID Token as a String
     */
    public String getSubject() {
        return (String) getPayload().get("sub");
    }

    /**
     * Returns the nonce claim value of the ID Token as a String or null if the ID Token did not contain that optional claim
     */
    public String getNonce() {
        return (String) getPayload().get("nonce");
    }

    /**
     * Returns the aud claim value of the ID Token as a List of Strings
     */
    public List<String> getAudiences() {
        return (List<String>) getPayload().get("aud");
    }

    /**
     * Returns the exp claim value of the ID Token as long
     */
    public long getExpiration() {
        return (long) getPayload().get("exp");
    }

    /**
     * Returns the iat claim value of the ID Token as long
     */
    public long getIssuedAt() {
        return (long) getPayload().get("iat");
    }

    /**
     * Returns the auth_time claim value or -1 if the ID Token did not contain that optional claim
     */
    public long getAuthTime() {
        if (getPayload().containsKey("auth_time")) {
            return (long) getPayload().get("auth_time");
        } else {
            return -1;
        }
    }

    /**
     * Returns the acr claim value or null if the ID Token did not contain that optional claim
     */
    public String getAuthenticationContectClassReference() {
        if (getPayload().containsKey("acr")) {
            return (String) getPayload().get("acr");
        } else {
            return null;
        }
    }

    /**
     * Returns the amr claim values as a List of Stings or an empty list if the ID Token did not contain that optional claim
     */
    public List<String> getAuthenticationMethodsReferences() {
        if (getPayload().containsKey("amr")) {
            return (List<String>) getPayload().get("amr");
        } else
            return Collections.emptyList();
    }

    /**
     * Returns the azp claim value or null if the ID Token did not contain that optional claim
     */
    public String getAuthorizedParty() {
        if (getPayload().containsKey("azp")) {
            return (String) getPayload().get("azp");
        } else {
            return null;
        }
    }
}
