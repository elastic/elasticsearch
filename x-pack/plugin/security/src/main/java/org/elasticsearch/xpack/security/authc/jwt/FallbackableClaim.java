/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jwt.JWTClaimsSet;

import org.elasticsearch.core.Nullable;

import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

/**
 * A JWT claim that can optionally fallback to another claim (if configured) for retrieving the associated value
 * from a {@link JWTClaimsSet}. The fallback behaviour happens only when:
 * 1. The fallback is configured (it can be null)
 * 2. The original claim does not exist in the {@link JWTClaimsSet}
 * In any other cases, the original claim will be used for retrieving the value.
 */
public class FallbackableClaim {
    private final String name;
    private final JWTClaimsSet claimsSet;
    private final String actualName;

    public FallbackableClaim(String name, @Nullable Map<String, String> fallbackClaimNames, JWTClaimsSet claimsSet) {
        this.name = Objects.requireNonNull(name);
        this.claimsSet = Objects.requireNonNull(claimsSet);
        final String fallbackName;
        if (fallbackClaimNames != null) {
            fallbackName = fallbackClaimNames.getOrDefault(name, name);
        } else {
            fallbackName = null;
        }
        if (fallbackName == null) {
            this.actualName = name;
        } else {
            this.actualName = claimsSet.getClaim(name) != null ? name : fallbackName;
        }
    }

    public String getActualName() {
        return actualName;
    }

    public String getStringClaimValue() {
        try {
            return claimsSet.getStringClaim(actualName);
        } catch (ParseException e) {
            throw new IllegalArgumentException(format("cannot parse string claim [%s] as string", this), e);
        }
    }

    public List<String> getStringListClaimValue() {
        final Object claimValue = claimsSet.getClaim(actualName);
        if (claimValue instanceof String) {
            return List.of((String) claimValue);
        } else {
            try {
                return claimsSet.getStringListClaim(actualName);
            } catch (ParseException e) {
                throw new IllegalArgumentException(format("cannot parse string claim [%s] as string array", this), e);
            }
        }
    }

    @Override
    public String toString() {
        if (name.equals(actualName)) {
            return name;
        } else {
            return format("%s (fallback of %s)", actualName, name);
        }
    }
}
