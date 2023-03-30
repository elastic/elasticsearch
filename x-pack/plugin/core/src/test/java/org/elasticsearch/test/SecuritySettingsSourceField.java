/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.test;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.Map;

public final class SecuritySettingsSourceField {
    public static final SecureString TEST_PASSWORD_SECURE_STRING = new SecureString("x-pack-test-password".toCharArray());
    public static final String TEST_PASSWORD = "x-pack-test-password";
    public static final String TEST_INVALID_PASSWORD = "invalid-test-password";

    public static final String ES_TEST_ROOT_ROLE = "_es_test_root";
    public static final String ES_TEST_ROOT_ROLE_YML = """
        _es_test_root:
          cluster: [ "ALL" ]
          indices:
            - names: [ "*" ]
              allow_restricted_indices: true
              privileges: [ "ALL" ]
          run_as: [ "*" ]
          applications:
            - application: "*"
              privileges: [ "*" ]
              resources: [ "*" ]
        """;

    public static final RoleDescriptor ES_TEST_ROOT_ROLE_DESCRIPTOR = new RoleDescriptor(
        "_es_test_root",
        new String[] { "ALL" },
        new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("ALL").allowRestrictedIndices(true).build() },
        new RoleDescriptor.ApplicationResourcePrivileges[] {
            RoleDescriptor.ApplicationResourcePrivileges.builder().application("*").privileges("*").resources("*").build() },
        null,
        new String[] { "*" },
        Map.of(),
        Map.of("enabled", true)
    );

    private SecuritySettingsSourceField() {}
}
