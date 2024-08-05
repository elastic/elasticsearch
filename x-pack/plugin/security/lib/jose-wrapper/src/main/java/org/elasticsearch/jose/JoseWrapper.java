/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.jose;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.SpecialPermission;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * This class wraps the operations requiring access in {@link AccessController#doPrivileged(PrivilegedAction)} blocks.
 * Can't do these operations inline with giving too much access due to how the security manager calculates the stack for lambda expressions.
 * Isolating the calls here allows for least privilege access to this helper jar.
 */
public class JoseWrapper {

    // utility class
    private JoseWrapper() {}

    public static String getHeaderAsString(SignedJWT signedJWT) {
        SpecialPermission.check();
        return AccessController.doPrivileged((PrivilegedAction<String>) () -> signedJWT.getHeader().toString());

    }

    public static String getClaimsSetAsString(JWTClaimsSet jwtClaimsSet) {
        SpecialPermission.check();
        return AccessController.doPrivileged((PrivilegedAction<String>) jwtClaimsSet::toString);
    }
}
