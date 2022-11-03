/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.text.ParseException;
import java.util.List;

public class JwtStringClaimValidator implements JwtClaimValidator {

    private final String claimName;
    private final List<String> allowedClaimValues;
    // Whether the claim should be a single string
    private final boolean singleValuedClaim;
    private final StringMatcher claimValueMatcher;

    public JwtStringClaimValidator(String claimName, List<String> allowedClaimValues, boolean singleValuedClaim) {
        this.claimName = claimName;
        this.allowedClaimValues = allowedClaimValues;
        this.singleValuedClaim = singleValuedClaim;
        // if (allowedClaimValues.stream().anyMatch(v -> v.startsWith("/") || v.contains("*"))) {
        // throw new ElasticsearchException("invalid allowed claim values, cannot use wildcard or regex");
        // }
        this.claimValueMatcher = StringMatcher.of(allowedClaimValues);
    }

    @Override
    public void validate(SignedJWT jwt) {
        final List<String> claimValues;
        try {
            claimValues = getStringClaimValues(jwt.getJWTClaimsSet());
        } catch (ParseException e) {
            throw new ElasticsearchSecurityException("cannot parse string claim [" + claimName + "]", RestStatus.BAD_REQUEST, e);
        }
        if (claimValues == null) {
            throw new ElasticsearchSecurityException("missing string claim [" + claimName + "]", RestStatus.BAD_REQUEST);
        }

        if (false == claimValues.stream().anyMatch(claimValueMatcher)) {
            throw new ElasticsearchSecurityException(
                "string claim ["
                    + claimName
                    + "] has value ["
                    + Strings.collectionToCommaDelimitedString(claimValues)
                    + "] which does not match allowed claim values ["
                    + Strings.collectionToCommaDelimitedString(allowedClaimValues)
                    + "]",
                RestStatus.BAD_REQUEST
            );
        }
    }

    private List<String> getStringClaimValues(JWTClaimsSet claimsSet) throws ParseException {
        // TODO: fallback claims
        final String actualClaimName = claimName;

        if (singleValuedClaim) {
            final String claimValue = claimsSet.getStringClaim(actualClaimName);
            return claimValue != null ? List.of(claimValue) : null;
        } else {
            final Object claimValue = claimsSet.getClaim(actualClaimName);
            if (claimValue instanceof String) {
                return List.of((String) claimValue);
            } else {
                return claimsSet.getStringListClaim(actualClaimName);
            }
        }
    }
}
