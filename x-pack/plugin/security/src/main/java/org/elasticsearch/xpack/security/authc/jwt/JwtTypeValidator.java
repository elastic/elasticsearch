/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.DefaultJOSEObjectTypeVerifier;
import com.nimbusds.jose.proc.JOSEObjectTypeVerifier;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;

public class JwtTypeValidator implements JwtFieldValidator {

    private static final JOSEObjectTypeVerifier<SecurityContext> JWT_HEADER_TYPE_VERIFIER = new DefaultJOSEObjectTypeVerifier<>(
        JOSEObjectType.JWT,
        null
    );

    public static final JwtTypeValidator INSTANCE = new JwtTypeValidator();

    private JwtTypeValidator() {}

    public void validate(JWSHeader jwsHeader, JWTClaimsSet jwtClaimsSet) {
        final JOSEObjectType jwtHeaderType = jwsHeader.getType();
        try {
            JWT_HEADER_TYPE_VERIFIER.verify(jwtHeaderType, null);
        } catch (BadJOSEException e) {
            throw new IllegalArgumentException("invalid jwt typ header", e);
        }
    }
}
