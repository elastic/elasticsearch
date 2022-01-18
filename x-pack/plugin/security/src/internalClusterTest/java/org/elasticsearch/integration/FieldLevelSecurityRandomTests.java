/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;

public class FieldLevelSecurityRandomTests extends SecurityIntegTestCase {

    protected static final SecureString USERS_PASSWD = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;

    private static Set<String> allowedFields;
    private static Set<String> disAllowedFields;

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(getFastStoredHashAlgoForTests().hash(USERS_PASSWD));

        return super.configUsers()
            + "user1:"
            + usersPasswdHashed
            + "\n"
            + "user2:"
            + usersPasswdHashed
            + "\n"
            + "user3:"
            + usersPasswdHashed
            + "\n"
            + "user4:"
            + usersPasswdHashed
            + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + """
            role1:user1,user2,user3,user4
            role2:user1
            role3:user2
            role4:user3
            role5:user4
            """;
    }

    @Override
    protected String configRoles() {
        if (allowedFields == null) {
            allowedFields = new HashSet<>();
            disAllowedFields = new HashSet<>();
            int numFields = scaledRandomIntBetween(5, 50);
            allowedFields.add("id");
            for (int i = 0; i < numFields; i++) {
                String field = "field" + i;
                if (i % 2 == 0) {
                    allowedFields.add(field);
                } else {
                    disAllowedFields.add(field);
                }
            }
        }

        StringBuilder roleFields = new StringBuilder();
        for (String field : allowedFields) {
            roleFields.append("          - ").append(field).append('\n');
        }

        return """
            %s
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
                     grant:
            %s
            role3:
              cluster:
                - all
              indices:
                - names: test
                  privileges:
                    - all
                  field_security:
                     grant: [ id, field1 ]
            role4:
              cluster: [ all ]
              indices:
                - names: test
                  privileges: [ ALL ]
                  field_security:
                     grant: [ id, field2 ]
            role5:
              cluster: [ all ]
              indices:
                - names: test
                  privileges: [ ALL ]
                  field_security:
                     grant: [ id, field3 ]
            """.formatted(super.configRoles(), roleFields.toString());
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(XPackSettings.DLS_FLS_ENABLED.getKey(), true)
            .build();
    }

    public void testRandom() {
        int j = 0;
        Map<String, Object> doc = new HashMap<>();
        String[] fieldMappers = new String[(allowedFields.size() + disAllowedFields.size()) * 2];
        for (String field : allowedFields) {
            fieldMappers[j++] = field;
            fieldMappers[j++] = "type=text";
            doc.put(field, "value");
        }
        for (String field : disAllowedFields) {
            fieldMappers[j++] = field;
            fieldMappers[j++] = "type=text";
            doc.put(field, "value");
        }
        assertAcked(client().admin().indices().prepareCreate("test").setMapping(fieldMappers));
        client().prepareIndex("test").setId("1").setSource(doc).setRefreshPolicy(IMMEDIATE).get();

        for (String allowedField : allowedFields) {
            logger.info("Checking allowed field [{}]", allowedField);
            SearchResponse response = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
            ).prepareSearch("test").setQuery(matchQuery(allowedField, "value")).get();
            assertHitCount(response, 1);
        }
        for (String disallowedField : disAllowedFields) {
            logger.info("Checking disallowed field [{}]", disallowedField);
            SearchResponse response = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
            ).prepareSearch("test").setQuery(matchQuery(disallowedField, "value")).get();
            assertHitCount(response, 0);
        }
    }

    public void testDuel() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setMapping("id", "type=keyword", "field1", "type=text", "field2", "type=text", "field3", "type=text")
        );

        int numDocs = scaledRandomIntBetween(32, 128);
        List<IndexRequestBuilder> requests = new ArrayList<>(numDocs);
        for (int i = 1; i <= numDocs; i++) {
            String field = randomFrom("field1", "field2", "field3");
            String value = "value";
            requests.add(client().prepareIndex("test").setId(value).setSource("id", Integer.toString(i), field, value));
        }
        indexRandom(true, requests);

        SearchResponse actual = client().filterWithHeader(
            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
        )
            .prepareSearch("test")
            .addSort("id", SortOrder.ASC)
            .setQuery(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("field1", "value"))
                    .should(QueryBuilders.termQuery("field2", "value"))
                    .should(QueryBuilders.termQuery("field3", "value"))
            )
            .get();
        SearchResponse expected = client().prepareSearch("test")
            .addSort("id", SortOrder.ASC)
            .setQuery(QueryBuilders.boolQuery().should(QueryBuilders.termQuery("field1", "value")))
            .get();
        assertThat(actual.getHits().getTotalHits().value, equalTo(expected.getHits().getTotalHits().value));
        assertThat(actual.getHits().getHits().length, equalTo(expected.getHits().getHits().length));
        for (int i = 0; i < actual.getHits().getHits().length; i++) {
            assertThat(actual.getHits().getAt(i).getId(), equalTo(expected.getHits().getAt(i).getId()));
        }

        actual = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
            .prepareSearch("test")
            .addSort("id", SortOrder.ASC)
            .setQuery(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("field1", "value"))
                    .should(QueryBuilders.termQuery("field2", "value"))
                    .should(QueryBuilders.termQuery("field3", "value"))
            )
            .get();
        expected = client().prepareSearch("test")
            .addSort("id", SortOrder.ASC)
            .setQuery(QueryBuilders.boolQuery().should(QueryBuilders.termQuery("field2", "value")))
            .get();
        assertThat(actual.getHits().getTotalHits().value, equalTo(expected.getHits().getTotalHits().value));
        assertThat(actual.getHits().getHits().length, equalTo(expected.getHits().getHits().length));
        for (int i = 0; i < actual.getHits().getHits().length; i++) {
            assertThat(actual.getHits().getAt(i).getId(), equalTo(expected.getHits().getAt(i).getId()));
        }

        actual = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
            .prepareSearch("test")
            .addSort("id", SortOrder.ASC)
            .setQuery(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.termQuery("field1", "value"))
                    .should(QueryBuilders.termQuery("field2", "value"))
                    .should(QueryBuilders.termQuery("field3", "value"))
            )
            .get();
        expected = client().prepareSearch("test")
            .addSort("id", SortOrder.ASC)
            .setQuery(QueryBuilders.boolQuery().should(QueryBuilders.termQuery("field3", "value")))
            .get();
        assertThat(actual.getHits().getTotalHits().value, equalTo(expected.getHits().getTotalHits().value));
        assertThat(actual.getHits().getHits().length, equalTo(expected.getHits().getHits().length));
        for (int i = 0; i < actual.getHits().getHits().length; i++) {
            assertThat(actual.getHits().getAt(i).getId(), equalTo(expected.getHits().getAt(i).getId()));
        }
    }

}
