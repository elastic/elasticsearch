/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;

public class DocumentLevelSecurityRandomTests extends SecurityIntegTestCase {

    protected static final SecureString USERS_PASSWD = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;

    // can't add a second test method, because each test run creates a new instance of this class and that will will result
    // in a new random value:
    private final int numberOfRoles = scaledRandomIntBetween(3, 99);

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(getFastStoredHashAlgoForTests().hash(USERS_PASSWD));

        StringBuilder builder = new StringBuilder(super.configUsers());
        for (int i = 1; i <= numberOfRoles; i++) {
            builder.append("user").append(i).append(':').append(usersPasswdHashed).append('\n');
        }
        return builder.toString();
    }

    @Override
    protected String configUsersRoles() {
        StringBuilder builder = new StringBuilder(super.configUsersRoles());
        builder.append("role0:");
        for (int i = 1; i <= numberOfRoles; i++) {
            builder.append("user").append(i);
            if (i != numberOfRoles) {
                builder.append(",");
            }
        }
        builder.append("\n");
        for (int i = 1; i <= numberOfRoles; i++) {
            builder.append("role").append(i).append(":user").append(i).append('\n');
        }
        return builder.toString();
    }

    @Override
    protected String configRoles() {
        StringBuilder builder = new StringBuilder(super.configRoles());
        builder.append("\nrole0:\n");
        builder.append("  cluster: [ none ]\n");
        builder.append("  indices:\n");
        builder.append("    - names: '*'\n");
        builder.append("      privileges: [ none ]\n");
        for (int i = 1; i <= numberOfRoles; i++) {
            builder.append("role").append(i).append(":\n");
            builder.append("  cluster: [ all ]\n");
            builder.append("  indices:\n");
            builder.append("    - names: '*'\n");
            builder.append("      privileges:\n");
            builder.append("        - all\n");
            builder.append("      query: \n");
            builder.append("        term: \n");
            builder.append("          field1: value").append(i).append('\n');
        }
        return builder.toString();
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.DLS_FLS_ENABLED.getKey(), true)
            .build();
    }

    public void testDuelWithAliasFilters() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setMapping("field1", "type=text", "field2", "type=text"));

        List<IndexRequestBuilder> requests = new ArrayList<>(numberOfRoles);
        IndicesAliasesRequestBuilder builder = client().admin().indices().prepareAliases();
        for (int i = 1; i <= numberOfRoles; i++) {
            String value = "value" + i;
            requests.add(client().prepareIndex("test").setId(value).setSource("field1", value));
            builder.addAlias("test", "alias" + i, QueryBuilders.termQuery("field1", value));
        }
        indexRandom(true, requests);
        builder.get();

        for (int roleI = 1; roleI <= numberOfRoles; roleI++) {
            SearchResponse searchResponse1 = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user" + roleI, USERS_PASSWD))
            ).prepareSearch("test").get();
            SearchResponse searchResponse2 = client().prepareSearch("alias" + roleI).get();
            assertThat(searchResponse1.getHits().getTotalHits().value, equalTo(searchResponse2.getHits().getTotalHits().value));
            for (int hitI = 0; hitI < searchResponse1.getHits().getHits().length; hitI++) {
                assertThat(searchResponse1.getHits().getAt(hitI).getId(), equalTo(searchResponse2.getHits().getAt(hitI).getId()));
            }
        }
    }

}
