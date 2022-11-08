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

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;

import java.util.List;

public class JwtHeaderValidator {

    private static final JOSEObjectTypeVerifier<SecurityContext> JWT_HEADER_TYPE_VERIFIER = new DefaultJOSEObjectTypeVerifier<>(
        JOSEObjectType.JWT,
        null
    );

    private final List<String> allowedAlgorithms;

    public JwtHeaderValidator(List<String> allowedAlgorithms) {
        this.allowedAlgorithms = allowedAlgorithms;
    }

    public void validate(JWSHeader jwsHeader) {

        final JOSEObjectType jwtHeaderType = jwsHeader.getType();
        try {
            JWT_HEADER_TYPE_VERIFIER.verify(jwtHeaderType, null);
        } catch (BadJOSEException e) {
            throw new ElasticsearchSecurityException("invalid jwt typ header", RestStatus.BAD_REQUEST, e);
        }

        final JWSAlgorithm algorithm = jwsHeader.getAlgorithm();
        if (algorithm == null) {
            throw new ElasticsearchSecurityException("missing JWT algorithm header", RestStatus.BAD_REQUEST);
        }

        if (false == allowedAlgorithms.contains(algorithm.getName())) {
            throw new ElasticsearchSecurityException(
                "invalid JWT algorithm [%s], allowed algorithms are [%s]",
                RestStatus.BAD_REQUEST,
                algorithm,
                Strings.collectionToCommaDelimitedString(allowedAlgorithms)
            );
        }
    }
}
