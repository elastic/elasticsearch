/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support.jwt;

import org.elasticsearch.common.Strings;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

public class JsonWebTokenBuilder {

    private String issuer;
    private String subject;
    private String audience;
    private ZonedDateTime expirationTime;
    private ZonedDateTime notBefore;
    private ZonedDateTime issuedAt;
    private String jwtId;
    private String type;
    private String algorithm;
    private Map<String, Object> claims;

    public JsonWebTokenBuilder issuer(String issuer) {
        this.issuer = issuer;
        return this;
    }

    public JsonWebTokenBuilder subject(String subject) {
        this.subject = subject;
        return this;
    }

    public JsonWebTokenBuilder audience(String audience) {
        this.audience = audience;
        return this;
    }

    public JsonWebTokenBuilder jwtId(String jwtId) {
        this.jwtId = jwtId;
        return this;
    }

    public JsonWebTokenBuilder expirationTime(ZonedDateTime expirationTime) {
        this.expirationTime = expirationTime;
        return this;
    }

    public JsonWebTokenBuilder notBefore(ZonedDateTime notBefore) {
        this.notBefore = notBefore;
        return this;
    }

    public JsonWebTokenBuilder issuedAt(ZonedDateTime issuedAt) {
        this.issuedAt = issuedAt;
        return this;
    }

    public JsonWebTokenBuilder type(String type) {
        if (Strings.hasText(type) == false) {
            throw new IllegalArgumentException("JWT type cannot be null or empty");
        }
        this.type = type;
        return this;
    }

    public JsonWebTokenBuilder algorithm(String algorithm) {
        if (Strings.hasText(algorithm) == false) {
            throw new IllegalArgumentException("JWT signing algorithm cannot be null or empty");
        }
        this.algorithm = algorithm;
        return this;
    }

    public JsonWebTokenBuilder claim(String name, Object value) {
        if (Strings.hasText(name) == false) {
            throw new IllegalArgumentException("Claim name cannot be null or empty");
        }
        getOrCreateClaims().put(name, value);
        return this;
    }

    private Map<String, Object> getOrCreateClaims() {
        if (claims == null) {
            claims = new HashMap<>();
            return claims;
        } else {
            return claims;
        }
    }

    public JsonWebToken build() {
        final Map<String, Object> header = new HashMap<>();
        header.put("typ", type);
        header.put("alg", algorithm);

        final Map<String, Object> payload = new HashMap<>();
        payload.put("iss", issuer);
        payload.put("sub", subject);
        payload.put("aud", audience);
        payload.put("exp", expirationTime);
        payload.put("nbf", notBefore);
        payload.put("iat", issuedAt);
        payload.put("jti", jwtId);
        if (claims != null) {
            payload.putAll(claims);
        }
        return new JsonWebToken(header, payload);
    }
}
