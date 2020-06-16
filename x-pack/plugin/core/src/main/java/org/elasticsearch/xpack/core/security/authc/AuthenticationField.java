/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc;

public final class AuthenticationField {

    public static final String AUTHENTICATION_KEY = "_xpack_security_authentication";
    public static final String API_KEY_ROLE_DESCRIPTORS_KEY = "_security_api_key_role_descriptors";
    public static final String API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY = "_security_api_key_limited_by_role_descriptors";

    private AuthenticationField() {}
}
