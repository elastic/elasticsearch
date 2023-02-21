/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jwt.JWTClaimsSet;

import org.elasticsearch.common.Strings;

import java.util.List;

import static org.elasticsearch.core.Strings.format;

public class JwtAlgorithmValidator implements JwtFieldValidator {

    private final List<String> allowedAlgorithms;

    public JwtAlgorithmValidator(List<String> allowedAlgorithms) {
        this.allowedAlgorithms = allowedAlgorithms;
    }

    public void validate(JWSHeader jwsHeader, JWTClaimsSet jwtClaimsSet) {
        final JWSAlgorithm algorithm = jwsHeader.getAlgorithm();
        if (algorithm == null) {
            throw new IllegalArgumentException("missing JWT algorithm header");
        }

        if (false == allowedAlgorithms.contains(algorithm.getName())) {
            throw new IllegalArgumentException(
                format(
                    "invalid JWT algorithm [%s], allowed algorithms are [%s]",
                    algorithm,
                    Strings.collectionToCommaDelimitedString(allowedAlgorithms)
                )
            );
        }
    }
}
