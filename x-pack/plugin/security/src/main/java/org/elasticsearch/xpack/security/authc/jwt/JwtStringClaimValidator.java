/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jwt.JWTClaimsSet;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Validates a specific string claim form a {@link JWTClaimsSet} against both a list of explicit values and a list of Lucene patterns.
 * The validation is successful if the claim's value matches any of the allowed values or patterns from the lists.
 * The {@link JWTClaimsSet} claim value can either be a single string or an array of strings.
 * The {@link JwtStringClaimValidator} can be configured to only accept a single string claim value
 * (and reject string array claims) when the {@link #singleValuedClaim} field is set to {@code true}.
 * When it is an array of string, the validation is successful when ANY array element matches ANY of the allowed values or patterns
 * (and {@link #singleValuedClaim} field is {@code false}).
 */
public class JwtStringClaimValidator implements JwtFieldValidator {

    // Allows any non-null value for the sub claim
    public static final JwtStringClaimValidator ALLOW_ALL_SUBJECTS = new JwtStringClaimValidator("sub", true, List.of(), List.of("*"));

    private final String claimName;
    // Whether the claim should be a single string
    private final boolean singleValuedClaim;
    @Nullable
    private final Map<String, String> fallbackClaimNames;
    private final Predicate<String> allowedClaimValuesPredicate;

    public JwtStringClaimValidator(
        String claimName,
        boolean singleValuedClaim,
        Collection<String> allowedClaimValues,
        Collection<String> allowedClaimValuePatterns
    ) {
        this(claimName, singleValuedClaim, null, allowedClaimValues, allowedClaimValuePatterns);
    }

    public JwtStringClaimValidator(
        String claimName,
        boolean singleValuedClaim,
        Map<String, String> fallbackClaimNames,
        Collection<String> allowedClaimValues,
        Collection<String> allowedClaimValuePatterns
    ) {
        assert allowedClaimValues != null : "allowed claim values should be empty rather than null";
        assert allowedClaimValuePatterns != null : "allowed claim value patterns should be empty rather than null";
        this.claimName = claimName;
        this.singleValuedClaim = singleValuedClaim;
        this.fallbackClaimNames = fallbackClaimNames;
        this.allowedClaimValuesPredicate = new Predicate<>() {
            private final Set<String> allowedClaimsSet = new HashSet<>(allowedClaimValues);
            private final Predicate<String> allowedClaimPatternsPredicate = predicateFromPatterns(claimName, allowedClaimValuePatterns);

            @Override
            public boolean test(String s) {
                return allowedClaimsSet.contains(s) || allowedClaimPatternsPredicate.test(s);
            }

            @Override
            public String toString() {
                return "[" + Strings.collectionToCommaDelimitedString(allowedClaimsSet) + "] || [" + allowedClaimPatternsPredicate + "]";
            }
        };
    }

    @Override
    public void validate(JWSHeader jwsHeader, JWTClaimsSet jwtClaimsSet) {
        final FallbackableClaim fallbackableClaim = new FallbackableClaim(claimName, fallbackClaimNames, jwtClaimsSet);
        final List<String> claimValues = getStringClaimValues(fallbackableClaim);
        if (claimValues == null) {
            throw new IllegalArgumentException("missing required string claim [" + fallbackableClaim + "]");
        }
        for (String claimValue : claimValues) {
            if (allowedClaimValuesPredicate.test(claimValue)) {
                return;
            }
        }
        throw new IllegalArgumentException(
            "string claim ["
                + fallbackableClaim
                + "] has value ["
                + Strings.collectionToCommaDelimitedString(claimValues)
                + "] which does not match allowed claim values "
                + allowedClaimValuesPredicate
        );
    }

    private List<String> getStringClaimValues(FallbackableClaim fallbackableClaim) {
        if (singleValuedClaim) {
            final String claimValue = fallbackableClaim.getStringClaimValue();
            return claimValue != null ? List.of(claimValue) : null;
        } else {
            return fallbackableClaim.getStringListClaimValue();
        }
    }

    private static Predicate<String> predicateFromPatterns(String claimName, Collection<String> patterns) {
        try {
            return Automatons.predicate(patterns);
        } catch (Exception e) {
            throw new SettingsException("Invalid patterns for allowed claim values for [" + claimName + "].", e);
        }
    }
}
