/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import com.nimbusds.jwt.JWTClaimsSet;

/**
 * Validator for the JWT Claims
 */
public interface JwtClaimValidator {

    /**
     * Validate the given claims. Throw exception if the validation fails.
     * @param jwtClaimsSet The claims set
     */
    void validate(JWTClaimsSet jwtClaimsSet);
}
