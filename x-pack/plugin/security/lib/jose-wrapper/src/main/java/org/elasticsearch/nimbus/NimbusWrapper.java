/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.nimbus;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.openid.connect.sdk.Nonce;
import com.nimbusds.openid.connect.sdk.validators.IDTokenValidator;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.SpecialPermission;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.text.ParseException;
import java.util.Map;

/**
 * This class wraps the operations requiring access in {@link AccessController#doPrivileged(PrivilegedAction)} blocks.
 * Can't do these operations inline with giving too much access due to how the security manager calculates the stack for lambda expressions.
 * Isolating the calls here allows for least privilege access to this helper jar.
 */
public class NimbusWrapper {

    // utility class
    private NimbusWrapper() {}

    public static String getHeaderAsString(SignedJWT signedJWT) {
        SpecialPermission.check();
        return AccessController.doPrivileged((PrivilegedAction<String>) () -> signedJWT.getHeader().toString());

    }

    public static String getClaimsSetAsString(JWTClaimsSet jwtClaimsSet) {
        SpecialPermission.check();
        return AccessController.doPrivileged((PrivilegedAction<String>) jwtClaimsSet::toString);
    }

    public static JWTClaimsSet verifyTokenClaims(IDTokenValidator validator, JWT idToken, Nonce nonce) throws BadJOSEException,
        JOSEException {
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<JWTClaimsSet>) () -> validator.validate(idToken, nonce).toJWTClaimsSet()
            );
        } catch (PrivilegedActionException exception) {
            if (exception.getCause() instanceof BadJOSEException e) {
                throw e;
            } else if (exception.getCause() instanceof JOSEException e) {
                throw e;
            } else {
                throw new ElasticsearchException(exception);
            }
        }
    }

    // only used in tests
    public static SignedJWT newSignedJwt(JWSHeader header, JWTClaimsSet claimsSet) {
        SpecialPermission.check();
        return AccessController.doPrivileged((PrivilegedAction<SignedJWT>) () -> new SignedJWT(header, claimsSet));
    }

    // only used in tests
    public static SignedJWT newSignedJWT(Map<String, Object> header, JWTClaimsSet claimsSet, String signatureUrl) throws ParseException {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged(
                (PrivilegedExceptionAction<SignedJWT>) () -> new SignedJWT(
                    JWSHeader.parse(header).toBase64URL(),
                    claimsSet.toPayload().toBase64URL(),
                    Base64URL.encode(signatureUrl)
                )
            );
        } catch (PrivilegedActionException ex) {
            throw (ParseException) ex.getException();
        }

    }
}
