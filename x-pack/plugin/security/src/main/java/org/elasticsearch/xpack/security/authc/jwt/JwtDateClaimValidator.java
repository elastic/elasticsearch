/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jwt.JWTClaimsSet;

import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;

import java.text.ParseException;
import java.time.Clock;
import java.time.Instant;
import java.util.Date;

public class JwtDateClaimValidator implements JwtFieldValidator {

    public enum Relationship {
        BEFORE_NOW,
        AFTER_NOW;
    }

    private final Clock clock;
    private final String claimName;
    private final long allowedClockSkewSeconds;
    private final Relationship relationship;
    private final boolean allowNull;

    public JwtDateClaimValidator(Clock clock, String claimName, TimeValue allowedClockSkew, Relationship relationship, boolean allowNull) {
        this.clock = clock;
        this.claimName = claimName;
        this.allowedClockSkewSeconds = allowedClockSkew.seconds();
        this.relationship = relationship;
        this.allowNull = allowNull;
    }

    @Override
    public void validate(JWSHeader jwsHeader, JWTClaimsSet jwtClaimsSet) {
        final Date claimValue;
        try {
            claimValue = jwtClaimsSet.getDateClaim(claimName);
        } catch (ParseException e) {
            throw new IllegalArgumentException("cannot parse date claim [" + claimName + "]", e);
        }

        if (claimValue == null) {
            if (allowNull) {
                return;
            } else {
                throw new IllegalArgumentException("missing required date claim [" + claimName + "]");
            }
        }

        final Instant claimInstant = claimValue.toInstant();
        final Instant now = clock.instant();

        switch (relationship) {
            case BEFORE_NOW:
                if (false == claimInstant.isBefore(now.plusSeconds(allowedClockSkewSeconds))) {
                    throw new IllegalArgumentException(
                        Strings.format(
                            "date claim [%s] value [%s] must be before now [%s]",
                            claimName,
                            claimInstant.toEpochMilli(),
                            now.toEpochMilli()
                        )
                    );
                }
                break;
            case AFTER_NOW:
                if (false == claimInstant.isAfter(now.minusSeconds(allowedClockSkewSeconds))) {
                    throw new IllegalArgumentException(
                        Strings.format(
                            "date claim [%s] value [%s] must be after now [%s]",
                            claimName,
                            claimInstant.toEpochMilli(),
                            now.toEpochMilli()
                        )
                    );
                }
                break;
            default:
                assert false : "unknown date claim relationship " + relationship;
                throw new IllegalStateException("unknown date claim relationship");
        }
    }
}
