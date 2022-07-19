/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateAction;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateRequest;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateResponse;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.NativeRealmIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import java.util.Collections;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

public class SecurityFeatureResetTests extends NativeRealmIntegTestCase {
    private static final SecureString SUPER_USER_PASSWD = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;

    @Override
    public boolean shouldDeleteSecurityIndex() {
        return false;
    }

    @Before
    public void setupForTests() throws Exception {
        // adds a dummy user to the native realm to force .security index creation
        new TestSecurityClient(getRestClient(), SecuritySettingsSource.SECURITY_REQUEST_OPTIONS).putUser(
            new User("dummy_user", "missing_role"),
            SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
        );
        assertSecurityIndexActive();
    }

    @Override
    protected String configUsers() {
        final String usersPasswHashed = new String(getFastStoredHashAlgoForTests().hash(SUPER_USER_PASSWD));
        return super.configUsers()
            + "su:"
            + usersPasswHashed
            + "\n"
            + "manager:"
            + usersPasswHashed
            + "\n"
            + "usr:"
            + usersPasswHashed
            + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + """
            superuser:su
            role1:manager
            role2:usr""";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + """
            %s
            role1:
              cluster: [ all ]
              indices:
                - names: '*'
                  privileges: [ manage ]
            role2:
              cluster: [ all ]
              indices:
                - names: '*'
                  privileges: [ read ]
            """;
    }

    public void testFeatureResetSuperuser() {
        assertReset("su", SUPER_USER_PASSWD, 0);
    }

    public void testFeatureResetManageRole() {
        assertReset("manager", SUPER_USER_PASSWD, 0);
    }

    /*public void testFeatureResetUsrRole() {
        assertReset("usr", SUPER_USER_PASSWD, 1);
    }*/

    private void assertReset(String user, SecureString password, final int numFailures) {
        final ResetFeatureStateRequest req = new ResetFeatureStateRequest();

        client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue(user, password)))
            .admin()
            .cluster()
            .execute(ResetFeatureStateAction.INSTANCE, req, new ActionListener<>() {
                @Override
                public void onResponse(ResetFeatureStateResponse response) {
                    long failures = response.getFeatureStateResetStatuses()
                        .stream()
                        .filter(status -> status.getStatus() == ResetFeatureStateResponse.ResetFeatureStateStatus.Status.FAILURE)
                        .count();
                    assertEquals(numFailures, failures);
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Shouldn't reach here");
                }
            });
    }
}
