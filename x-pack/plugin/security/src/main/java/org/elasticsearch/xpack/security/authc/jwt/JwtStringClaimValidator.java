/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jwt.JWTClaimsSet;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.text.ParseException;
import java.util.List;

/**
 * Validates a string claim against a list of allowed values. The validation is successful
 * if the claim's value matches any of the allowed values.
 * The claim's value can be either a single string or an array of strings. When it is an array
 * of string, the validation passes when any member of the string array matches any of the allowed
 * values.
 * Whether a claim's value can be an array of strings is customised with the {@link #singleValuedClaim}
 * field, which enforces the claim's value to be a single string if it is configured to {@code true}.
 */
public class JwtStringClaimValidator implements JwtFieldValidator {

    private final String claimName;
    private final List<String> allowedClaimValues;
    // Whether the claim should be a single string
    private final boolean singleValuedClaim;
    private final StringMatcher claimValueMatcher;

    public JwtStringClaimValidator(String claimName, List<String> allowedClaimValues, boolean singleValuedClaim) {
        this.claimName = claimName;
        this.allowedClaimValues = allowedClaimValues;
        this.singleValuedClaim = singleValuedClaim;
        this.claimValueMatcher = StringMatcher.of(allowedClaimValues);
    }

    @Override
    public void validate(JWSHeader jwsHeader, JWTClaimsSet jwtClaimsSet) {
        final List<String> claimValues;
        try {
            claimValues = getStringClaimValues(jwtClaimsSet);
        } catch (ParseException e) {
            throw new ElasticsearchSecurityException("cannot parse string claim [" + claimName + "]", RestStatus.BAD_REQUEST, e);
        }
        if (claimValues == null) {
            throw new ElasticsearchSecurityException("missing required string claim [" + claimName + "]", RestStatus.BAD_REQUEST);
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
