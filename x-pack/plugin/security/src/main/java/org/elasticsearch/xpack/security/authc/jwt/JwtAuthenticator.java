/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.text.ParseException;
import java.time.Clock;
import java.util.List;

/**
 * This class performs validations of header, claims and signatures against the incoming {@link JwtAuthenticationToken}.
 * It returns the {@link JWTClaimsSet} associated to the token if validation is successful.
 * Note this class does not care about users nor its caching behaviour.
 */
public class JwtAuthenticator implements Releasable {

    private static final Logger logger = LogManager.getLogger(JwtAuthenticator.class);
    private final RealmConfig realmConfig;
    private final List<JwtClaimValidator> jwtClaimValidators;
    private final JwtSignatureValidator jwtSignatureValidator;
    private final JwtHeaderValidator jwtHeaderValidator;

    public JwtAuthenticator(
        final RealmConfig realmConfig,
        final SSLService sslService,
        final JwtSignatureValidator.PkcJwkSetReloadNotifier reloadNotifier
    ) {
        this.realmConfig = realmConfig;
        final TimeValue allowedClockSkew = realmConfig.getSetting(JwtRealmSettings.ALLOWED_CLOCK_SKEW);
        final Clock clock = Clock.systemUTC();
        this.jwtClaimValidators = List.of(
            new JwtStringClaimValidator("iss", List.of(realmConfig.getSetting(JwtRealmSettings.ALLOWED_ISSUER)), true),
            new JwtStringClaimValidator("aud", realmConfig.getSetting(JwtRealmSettings.ALLOWED_AUDIENCES), false),
            new JwtDateClaimValidator(clock, "iat", allowedClockSkew, JwtDateClaimValidator.Relationship.BEFORE_NOW, false),
            new JwtDateClaimValidator(clock, "exp", allowedClockSkew, JwtDateClaimValidator.Relationship.AFTER_NOW, false),
            new JwtDateClaimValidator(clock, "nbf", allowedClockSkew, JwtDateClaimValidator.Relationship.BEFORE_NOW, true),
            new JwtDateClaimValidator(clock, "auth_time", allowedClockSkew, JwtDateClaimValidator.Relationship.BEFORE_NOW, true)
        );

        this.jwtHeaderValidator = new JwtHeaderValidator(realmConfig.getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS));
        this.jwtSignatureValidator = new JwtSignatureValidator.DelegatingJwtSignatureValidator(realmConfig, sslService, reloadNotifier);
    }

    public void authenticate(JwtAuthenticationToken jwtAuthenticationToken, ActionListener<JWTClaimsSet> listener) {
        final String tokenPrincipal = jwtAuthenticationToken.principal();

        // JWT cache
        final SecureString serializedJwt = jwtAuthenticationToken.getEndUserSignedJwt();
        final SignedJWT signedJWT;
        try {
            signedJWT = SignedJWT.parse(serializedJwt.toString());
        } catch (ParseException e) {
            // TODO: No point to continue to another realm since parsing failed
            listener.onFailure(e);
            return;
        }

        final JWTClaimsSet jwtClaimsSet;
        try {
            jwtClaimsSet = signedJWT.getJWTClaimsSet();
        } catch (ParseException e) {
            // TODO: No point to continue to another realm since get claimset failed
            listener.onFailure(e);
            return;
        }

        if (logger.isDebugEnabled()) {
            logger.debug(
                "Realm [{}] successfully parsed JWT token [{}] with header [{}] and claimSet [{}]",
                realmConfig.name(),
                tokenPrincipal,
                signedJWT.getHeader(),
                jwtClaimsSet
            );
        }

        for (JwtClaimValidator jwtClaimValidator : jwtClaimValidators) {
            try {
                jwtClaimValidator.validate(jwtClaimsSet);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
        }

        try {
            jwtHeaderValidator.validate(signedJWT.getHeader());
            jwtSignatureValidator.validate(tokenPrincipal, signedJWT, listener.map(ignored -> jwtClaimsSet));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void close() {
        jwtSignatureValidator.close();
    }

    // Package private for testing
    JwtSignatureValidator.DelegatingJwtSignatureValidator getJwtSignatureValidator() {
        assert jwtSignatureValidator instanceof JwtSignatureValidator.DelegatingJwtSignatureValidator;
        return (JwtSignatureValidator.DelegatingJwtSignatureValidator) jwtSignatureValidator;
    }
}
