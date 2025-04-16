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

    private final JOSEObjectTypeVerifier<SecurityContext> JWT_HEADER_TYPE_VERIFIER;
    private static final JOSEObjectType AT_PLUS_JWT = new JOSEObjectType("at+jwt");

    public static final JwtTypeValidator ID_TOKEN_INSTANCE = new JwtTypeValidator(JOSEObjectType.JWT, null);

    // strictly speaking, this should only permit `at+jwt`, but removing the other two options is a breaking change
    public static final JwtTypeValidator ACCESS_TOKEN_INSTANCE = new JwtTypeValidator(JOSEObjectType.JWT, AT_PLUS_JWT, null);

    private JwtTypeValidator(JOSEObjectType... allowedTypes) {
        JWT_HEADER_TYPE_VERIFIER = new DefaultJOSEObjectTypeVerifier<>(allowedTypes);
    }

    public void validate(JWSHeader jwsHeader, JWTClaimsSet jwtClaimsSet) {
        final JOSEObjectType jwtHeaderType = jwsHeader.getType();
        try {
            JWT_HEADER_TYPE_VERIFIER.verify(jwtHeaderType, null);
        } catch (BadJOSEException e) {
            throw new IllegalArgumentException("invalid jwt typ header", e);
        }
    }
}
