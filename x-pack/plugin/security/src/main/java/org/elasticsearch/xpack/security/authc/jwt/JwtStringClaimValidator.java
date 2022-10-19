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
import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.text.ParseException;
import java.util.List;

public class JwtStringClaimValidator implements JwtClaimValidator {

    private final String claimName;
    // Whether the claim should be a single string
    private final boolean singleValuedClaim;
    private final StringMatcher claimValueMatcher;

    public JwtStringClaimValidator(String claimName, List<String> allowedClaimValues, boolean singleValuedClaim) {
        this.claimName = claimName;
        this.singleValuedClaim = singleValuedClaim;
//        if (allowedClaimValues.stream().anyMatch(v -> v.startsWith("/") || v.contains("*"))) {
//            throw new ElasticsearchException("invalid allowed claim values, cannot use wildcard or regex");
//        }
        this.claimValueMatcher = StringMatcher.of(allowedClaimValues);
    }

    @Override
    public void validate(SignedJWT jwt) {
        final List<String> claimValues;
        try {
            claimValues = getStringClaimValues(jwt.getJWTClaimsSet());
        } catch (ParseException e) {
            throw new ElasticsearchSecurityException("string parsing failed: claim [" + claimName + "]", e);
        }
        if (claimValues == null) {
            throw new ElasticsearchSecurityException("validation failed: claim [" + claimName + "] does not exist");
        }

        if (false == claimValues.stream().anyMatch(claimValueMatcher)) {
            throw new ElasticsearchSecurityException(
                "validation failed: claim ["
                    + claimName
                    + "] value ["
                    + Strings.collectionToCommaDelimitedString(claimValues)
                    + "] do not match"
            );
        }
    }

    private List<String> getStringClaimValues(JWTClaimsSet claimsSet) throws ParseException {
        // TODO: fallback claims
        final String actualClaimName = claimName;

        if (singleValuedClaim) {
            return List.of(claimsSet.getStringClaim(actualClaimName));
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
