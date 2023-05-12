/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;

import java.util.Collections;

import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationException;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

public class AnalyzeTests extends SecurityIntegTestCase {

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(
            getFastStoredHashAlgoForTests().hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
        );
        return super.configUsers() + "analyze_indices:" + usersPasswdHashed + "\n" + "analyze_cluster:" + usersPasswdHashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + "analyze_indices:analyze_indices\n" + "analyze_cluster:analyze_cluster\n";
    }

    @Override
    protected String configRoles() {
        // role that has analyze indices privileges only
        return Strings.format("""
            %s
            analyze_indices:
              indices:
                - names: 'test_*'
                  privileges: [ 'indices:admin/analyze' ]
            analyze_cluster:
              cluster:
                - cluster:admin/analyze
            """, super.configRoles());
    }

    public void testAnalyzeWithIndices() {
        // this test tries to execute different analyze api variants from a user that has analyze privileges only on a specific index
        // namespace

        createIndex("test_1");
        ensureGreen();

        // ok: user has permissions for analyze on test_*
        SecureString passwd = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
        client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("analyze_indices", passwd)))
            .admin()
            .indices()
            .prepareAnalyze("this is my text")
            .setIndex("test_1")
            .setAnalyzer("standard")
            .get();

        // fails: user doesn't have permissions for analyze on index non_authorized
        assertThrowsAuthorizationException(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("analyze_indices", passwd)))
                .admin()
                .indices()
                .prepareAnalyze("this is my text")
                .setIndex("non_authorized")
                .setAnalyzer("standard")::get,
            AnalyzeAction.NAME,
            "analyze_indices"
        );

        // fails: user doesn't have permissions for cluster level analyze
        assertThrowsAuthorizationException(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("analyze_indices", passwd)))
                .admin()
                .indices()
                .prepareAnalyze("this is my text")
                .setAnalyzer("standard")::get,
            "cluster:admin/analyze",
            "analyze_indices"
        );
    }

    public void testAnalyzeWithoutIndices() {
        // this test tries to execute different analyze api variants from a user that has analyze privileges only at cluster level

        SecureString passwd = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
        // fails: user doesn't have permissions for analyze on index test_1
        assertThrowsAuthorizationException(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("analyze_cluster", passwd)))
                .admin()
                .indices()
                .prepareAnalyze("this is my text")
                .setIndex("test_1")
                .setAnalyzer("standard")::get,
            AnalyzeAction.NAME,
            "analyze_cluster"
        );

        client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("analyze_cluster", passwd)))
            .admin()
            .indices()
            .prepareAnalyze("this is my text")
            .setAnalyzer("standard")
            .get();
    }
}
