/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.SecurityProfileUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.nio.file.Path;

public class InternalUserAndRoleIntegTests extends AbstractPrivilegeTestCase {
    private static final String[] INTERNAL_USER_NAMES = new String[] {
        SystemUser.INSTANCE.principal(),
        XPackUser.INSTANCE.principal(),
        XPackSecurityUser.INSTANCE.principal(),
        AsyncSearchUser.INSTANCE.principal(),
        SecurityProfileUser.INSTANCE.principal() };

    private static final String ROLES = """
        _system:
          cluster: [ none ]
          indices:
            - names: 'a'
              privileges: [ all ]

        _xpack:
          cluster: [ none ]
          indices:
            - names: 'a'
              privileges: [ all ]

        _xpack_security:
          cluster: [ none ]
          indices:
            - names: 'a'
              privileges: [ all ]

        _security_profile:
          cluster: [ none ]
          indices:
            - names: 'a'
              privileges: [ all ]

        _async_search:
          cluster: [ none ]
          indices:
            - names: 'a'
              privileges: [ all ]

        _custom_role:
          indices:
            - names: 'a'
              privileges: [ all ]
        """;

    private static final String USERS_ROLES = """
        _system:user
        _xpack:user
        _xpack_security:user
        _security_profile:user
        _async_search:user
        _custom_role:_system,_xpack,_xpack_security,_security_profile,_async_search
        """;

    @Override
    protected String configRoles() {
        return super.configRoles() + "\n" + ROLES;
    }

    @Override
    protected String configUsers() {
        final Hasher passwdHasher = getFastStoredHashAlgoForTests();
        final String usersPasswdHashed = new String(passwdHasher.hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
        return super.configUsers()
            + "user:"
            + usersPasswdHashed
            + "\n"
            + "_system:"
            + usersPasswdHashed
            + "\n"
            + "_xpack:"
            + usersPasswdHashed
            + "\n"
            + "_security_profile:"
            + usersPasswdHashed
            + "\n"
            + "_xpack_security:"
            + usersPasswdHashed
            + "\n"
            + "_async_search:"
            + usersPasswdHashed
            + "\n";
    }

    private static Path repositoryLocation;

    @BeforeClass
    public static void setupRepositoryPath() {
        repositoryLocation = createTempDir();
    }

    @AfterClass
    public static void cleanupRepositoryPath() {
        repositoryLocation = null;
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put("path.repo", repositoryLocation).build();
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + USERS_ROLES;
    }

    public void testInternalRoleNamesDoNotResultInInternalUserPermissions() throws Exception {
        assertAccessIsDenied("user", "GET", "/_cluster/health");
        assertAccessIsDenied("user", "PUT", "/" + XPackPlugin.ASYNC_RESULTS_INDEX + "/_doc/1", "{}");
        assertAccessIsDenied("user", "PUT", "/.security-profile/_doc/1", "{}");
        assertAccessIsAllowed("user", "PUT", "/a/_doc/1", "{}");
    }

    public void testInternalUsernamesDoNotResultInInternalUserPermissions() throws Exception {
        for (final var internalUsername : INTERNAL_USER_NAMES) {
            assertAccessIsDenied(internalUsername, "GET", "/_cluster/health");
            assertAccessIsDenied("user", "PUT", "/" + XPackPlugin.ASYNC_RESULTS_INDEX + "/_doc/1", "{}");
            assertAccessIsDenied("user", "PUT", "/.security-profile/_doc/1", "{}");
            assertAccessIsAllowed(internalUsername, "PUT", "/a/_doc/1", "{}");
        }
    }
}
