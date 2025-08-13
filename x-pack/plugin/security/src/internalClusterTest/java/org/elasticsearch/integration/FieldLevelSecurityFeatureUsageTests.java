/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Collections;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

@ESIntegTestCase.ClusterScope(numClientNodes = 1)
public class FieldLevelSecurityFeatureUsageTests extends AbstractDocumentAndFieldLevelSecurityTests {

    protected static final SecureString USERS_PASSWD = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(getFastStoredHashAlgoForTests().hash(USERS_PASSWD));
        return super.configUsers() + Strings.format("""
            user1:%s
            """, usersPasswdHashed);
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + """
            role1:user1
            role2:user1
            """;
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + """

            role1:
              cluster: [ none ]
              indices:
                - names: '*'
                  privileges: [ none ]
            role2:
              cluster: [ all ]
              indices:
                  - names: '*'
                    privileges: [ ALL ]
                    field_security:
                       grant: [ field1 ]
            """;
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.DLS_FLS_ENABLED.getKey(), true)
            .put(XPackSettings.AUDIT_ENABLED.getKey(), false) // Just to make logs less noisy
            .build();
    }

    public void testFlsFeatureUsageTracking() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field1", "type=text", "field2", "type=text"));
        prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("2").setSource("field1", "value2", "field2", "value2").setRefreshPolicy(IMMEDIATE).get();

        assertHitCount(
            internalCluster().coordOnlyNodeClient()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(QueryBuilders.termQuery("field1", "value1")),
            1
        );

        // coordinating only node should not tack DLS/FLS feature usage
        assertDlsFlsNotTrackedOnCoordOnlyNode();
        // only FLS feature should have been tracked across all data nodes
        assertOnlyFlsTracked();
    }

}
