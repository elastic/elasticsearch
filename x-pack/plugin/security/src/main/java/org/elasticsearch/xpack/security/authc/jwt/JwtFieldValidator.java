/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jwt.JWTClaimsSet;

/**
 * Validator for fields (header or claim) of a JWT token
 */
public interface JwtFieldValidator {

    /**
     * Validate the given header and claims. Throw exception if the validation fails.
     * @param jwsHeader The header section of a JWT
     * @param jwtClaimsSet The claims set section of a JWT
     */
    void validate(JWSHeader jwsHeader, JWTClaimsSet jwtClaimsSet);
}
