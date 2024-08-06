/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.nimbus;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

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
public class NimubsWrapper {

    // utility class
    private NimubsWrapper() {}

    public static String getHeaderAsString(SignedJWT signedJWT) {
        SpecialPermission.check();
        return AccessController.doPrivileged((PrivilegedAction<String>) () -> signedJWT.getHeader().toString());

    }

    public static String getClaimsSetAsString(JWTClaimsSet jwtClaimsSet) {
        SpecialPermission.check();
        return AccessController.doPrivileged((PrivilegedAction<String>) jwtClaimsSet::toString);
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
