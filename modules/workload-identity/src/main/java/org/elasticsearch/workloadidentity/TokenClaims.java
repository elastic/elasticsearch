/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.workloadidentity;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.Base64;
import java.util.Map;

/**
 * Best-effort extraction of non-secret identifier claims from a compact JWT, for audit
 * correlation only: no signature verification (the token arrives from our issuer over mTLS) and
 * nothing grants on these values. Malformed input yields {@code null} fields, never an error.
 *
 * @param subject   the {@code sub} claim, or {@code null}
 * @param sessionId the {@code jti} claim (the issuer's per-token session id), or {@code null}
 */
record TokenClaims(@Nullable String subject, @Nullable String sessionId) {

    static final TokenClaims EMPTY = new TokenClaims(null, null);

    /** Decodes the payload segment of {@code token}; returns {@link #EMPTY} on any parse failure. */
    static TokenClaims decode(String token) {
        try {
            final int firstDot = token.indexOf('.');
            final int secondDot = token.indexOf('.', firstDot + 1);
            if (firstDot < 0 || secondDot < 0) {
                return EMPTY;
            }
            final byte[] payload = Base64.getUrlDecoder().decode(token.substring(firstDot + 1, secondDot));
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, payload)) {
                final Map<String, Object> claims = parser.map();
                return new TokenClaims(stringClaim(claims, "sub"), stringClaim(claims, "jti"));
            }
        } catch (Exception e) {
            return EMPTY;
        }
    }

    @Nullable
    private static String stringClaim(Map<String, Object> claims, String name) {
        return claims.get(name) instanceof String value && value.isEmpty() == false ? value : null;
    }
}
