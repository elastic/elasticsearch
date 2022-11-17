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

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;

import java.util.List;

public class JwtAlgorithmValidator implements JwtFieldValidator {

    private final List<String> allowedAlgorithms;

    public JwtAlgorithmValidator(List<String> allowedAlgorithms) {
        this.allowedAlgorithms = allowedAlgorithms;
    }

    public void validate(JWSHeader jwsHeader, JWTClaimsSet jwtClaimsSet) {
        final JWSAlgorithm algorithm = jwsHeader.getAlgorithm();
        if (algorithm == null) {
            throw new ElasticsearchSecurityException("missing JWT algorithm header", RestStatus.BAD_REQUEST);
        }

        if (false == allowedAlgorithms.contains(algorithm.getName())) {
            throw new ElasticsearchSecurityException(
                "invalid JWT algorithm [{}], allowed algorithms are [{}]",
                RestStatus.BAD_REQUEST,
                algorithm,
                Strings.collectionToCommaDelimitedString(allowedAlgorithms)
            );
        }
    }
}
