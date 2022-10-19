/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.core.TimeValue;

import java.text.ParseException;
import java.time.Clock;
import java.time.Instant;
import java.util.Date;

public class JwtTimeClaimValidator implements JwtClaimValidator {

    public enum Relationship {
        BEFORE_NOW,
        AFTER_NOW;
    }

    private final Clock clock;
    private final String claimName;
    private final long allowedClockSkewSeconds;
    private final Relationship relationship;
    private final boolean allowNull;

    public JwtTimeClaimValidator(Clock clock, String claimName, TimeValue allowedClockSkew, Relationship relationship, boolean allowNull) {
        this.clock = clock;
        this.claimName = claimName;
        this.allowedClockSkewSeconds = allowedClockSkew.seconds();
        this.relationship = relationship;
        this.allowNull = allowNull;
    }

    @Override
    public void validate(SignedJWT jwt) {
        final Date claimValue;
        try {
            claimValue = jwt.getJWTClaimsSet().getDateClaim(claimName);
        } catch (ParseException e) {
            throw new ElasticsearchSecurityException("date parsing failed: claim [" + claimName + "]", e);
        }

        if (claimValue == null) {
            if (allowNull) {
                return;
            } else {
                throw new ElasticsearchSecurityException("validation failed: claim [" + claimName + "] does not exist");
            }
        }

        final Instant claimInstant = claimValue.toInstant();
        // TODO: pass in clock
        final Instant now = clock.instant();

        switch (relationship) {
            case BEFORE_NOW:
                if (false == claimInstant.isBefore(now.plusSeconds(allowedClockSkewSeconds))) {
                    throw new ElasticsearchSecurityException("validation failed: claim [" + claimName + "] must be before now");
                }
                break;
            case AFTER_NOW:
                if (false == claimInstant.isAfter(now.minusSeconds(allowedClockSkewSeconds))) {
                    throw new ElasticsearchSecurityException("validation failed: claim [" + claimName + "] must be after now");
                }
                break;
            default:
                throw new IllegalStateException("unknown date claim relationship");
        }
    }
}
