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
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;

import java.util.Collections;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

public class SecurityFeatureResetTests extends SecurityIntegTestCase {
    private static final SecureString SUPER_USER_PASSWD = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;

    @Override
    protected String configUsers() {
        final String usersPasswHashed = new String(getFastStoredHashAlgoForTests().hash(SUPER_USER_PASSWD));
        return super.configUsers() + "su:" + usersPasswHashed + "\n" + "manager:" + usersPasswHashed + "\n";

    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + """
            superuser:su
            role1:manager""";
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
            """;
    }

    public void testFeatureResetSuperuser() {
        assertCanReset("su", SUPER_USER_PASSWD);
    }

    public void testFeatureResetManageRole() {
        assertCanReset("manager", SUPER_USER_PASSWD);
    }

    private void assertCanReset(String user, SecureString password) {
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
                    assertEquals(0, failures);
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Shouldn't reach here");
                }
            });
    }
}
