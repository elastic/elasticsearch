/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.DefaultJOSEObjectTypeVerifier;
import com.nimbusds.jose.proc.JOSEObjectTypeVerifier;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.SignedJWT;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;

import java.util.List;

public class JwtHeaderValidator implements JwtClaimValidator {

    private static final JOSEObjectTypeVerifier<SecurityContext> JWT_HEADER_TYPE_VERIFIER = new DefaultJOSEObjectTypeVerifier<>(
        JOSEObjectType.JWT,
        null
    );

    private final List<String> allowedAlgorithms;

    public JwtHeaderValidator(List<String> allowedAlgorithms) {
        this.allowedAlgorithms = allowedAlgorithms;
    }

    @Override
    public void validate(SignedJWT jwt) {

        final JWSHeader jwtHeader = jwt.getHeader();
        final JOSEObjectType jwtHeaderType = jwtHeader.getType();
        try {
            JWT_HEADER_TYPE_VERIFIER.verify(jwtHeaderType, null);
        } catch (BadJOSEException e) {
            throw new ElasticsearchException("validation failed: type header", e);
        }

        final JWSAlgorithm algorithm = jwt.getHeader().getAlgorithm();
        if (algorithm == null) {
            throw new ElasticsearchSecurityException("JWT algorithm is null");
        }

        if (false == allowedAlgorithms.contains(algorithm.getName())) {
            throw new ElasticsearchException(
                "Algorithm [%s] is invalid because it is not one of the allowed algorithms [%s]",
                algorithm,
                Strings.collectionToCommaDelimitedString(allowedAlgorithms)
            );
        }
    }
}
