/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import joptsimple.internal.Strings;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.SecurityProfileUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.UsernamesField;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.nio.file.Path;

public class InternalUserAndRoleIntegTests extends AbstractPrivilegeTestCase {
    private static final String[] INTERNAL_USERNAMES = new String[] {
        SystemUser.NAME,
        XPackUser.NAME,
        XPackSecurityUser.NAME,
        AsyncSearchUser.NAME,
        SecurityProfileUser.NAME };

    private static final String[] INTERNAL_ROLE_NAMES = new String[] {
        SystemUser.ROLE_NAME,
        UsernamesField.XPACK_ROLE,
        UsernamesField.XPACK_SECURITY_ROLE,
        UsernamesField.ASYNC_SEARCH_ROLE,
        UsernamesField.SECURITY_PROFILE_ROLE };

    @Override
    protected String configRoles() {
        StringBuilder builder = new StringBuilder(super.configRoles());
        for (String roleName : INTERNAL_ROLE_NAMES) {
            builder.append(defaultRole(roleName));
        }
        builder.append(defaultRole("_custom_role"));
        return builder.toString();
    }

    private String defaultRole(String roleName) {
        return """
            %s:
              cluster: [ none ]
              indices:
                - names: 'a'
                  privileges: [ all ]
            """.formatted(roleName);
    }

    @Override
    protected String configUsers() {
        final Hasher passwdHasher = getFastStoredHashAlgoForTests();
        final String usersPasswdHashed = new String(passwdHasher.hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
        StringBuilder builder = new StringBuilder(super.configUsers());
        for (String username : INTERNAL_USERNAMES) {
            builder.append(username).append(":").append(usersPasswdHashed).append("\n");
        }
        return builder + "user:" + usersPasswdHashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        StringBuilder builder = new StringBuilder(super.configUsersRoles());
        for (String roleName : INTERNAL_ROLE_NAMES) {
            builder.append("%s:%s\n".formatted(roleName, "user"));
        }
        // all internal users are mapped to custom role
        return builder + "%s:%s\n".formatted("_custom_role", Strings.join(INTERNAL_USERNAMES, ","));
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

    public void testInternalRoleNamesDoNotResultInInternalUserPermissions() throws Exception {
        assertAccessIsDenied("user", "GET", "/_cluster/health");
        assertAccessIsDenied("user", "PUT", "/" + XPackPlugin.ASYNC_RESULTS_INDEX + "/_doc/1", "{}");
        assertAccessIsDenied("user", "PUT", "/.security-profile/_doc/1", "{}");
        assertAccessIsAllowed("user", "PUT", "/a/_doc/1", "{}");
    }

    public void testInternalUsernamesDoNotResultInInternalUserPermissions() throws Exception {
        for (final var internalUsername : INTERNAL_USERNAMES) {
            assertAccessIsDenied(internalUsername, "GET", "/_cluster/health");
            assertAccessIsDenied("user", "PUT", "/" + XPackPlugin.ASYNC_RESULTS_INDEX + "/_doc/1", "{}");
            assertAccessIsDenied("user", "PUT", "/.security-profile/_doc/1", "{}");
            assertAccessIsAllowed(internalUsername, "PUT", "/a/_doc/1", "{}");
        }
    }
}
