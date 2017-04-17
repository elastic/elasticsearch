/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.authc.support.Hasher;
import org.elasticsearch.test.SecurityIntegTestCase;

import java.util.Collections;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.equalTo;

public class DocumentAndFieldLevelSecurityTests extends SecurityIntegTestCase {

    protected static final SecureString USERS_PASSWD = new SecureString("change_me".toCharArray());
    protected static final String USERS_PASSWD_HASHED = new String(Hasher.BCRYPT.hash(USERS_PASSWD));

    @Override
    protected String configUsers() {
        return super.configUsers() +
                "user1:" + USERS_PASSWD_HASHED + "\n" +
                "user2:" + USERS_PASSWD_HASHED + "\n" +
                "user3:" + USERS_PASSWD_HASHED + "\n" +
                "user4:" + USERS_PASSWD_HASHED + "\n" +
                "user5:" + USERS_PASSWD_HASHED + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() +
                "role1:user1\n" +
                "role2:user1,user4\n" +
                "role3:user2,user4\n" +
                "role4:user3,user4,user5\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() +
                "\nrole1:\n" +
                "  cluster: [ none ]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ none ]\n" +
                "role2:\n" +
                "  cluster:\n" +
                "   - all\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ ALL ]\n" +
                "      field_security:\n" +
                "         grant: [ field1 ]\n" +
                "      query: '{\"term\" : {\"field1\" : \"value1\"}}'\n" +
                "role3:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ ALL ]\n" +
                "      field_security:\n" +
                "         grant: [ field2 ]\n" +
                "      query: '{\"term\" : {\"field2\" : \"value2\"}}'\n" +
                "role4:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ ALL ]\n" +
                "      field_security:\n" +
                "         grant: [ field1 ]\n" +
                "      query: '{\"term\" : {\"field2\" : \"value2\"}}'\n";
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(XPackSettings.DLS_FLS_ENABLED.getKey(), true)
                .build();
    }

    public void testSimpleQuery() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text", "field2", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value2")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        SearchResponse response = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "1");
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).getSourceAsMap().get("field1").toString(), equalTo("value1"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "2");
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).getSourceAsMap().get("field2").toString(), equalTo("value2"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test")
                .addSort("_uid", SortOrder.ASC)
                .get();
        assertHitCount(response, 2);
        assertSearchHits(response, "1", "2");
        assertThat(response.getHits().getAt(0).getSourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getHits().getAt(1).getSourceAsMap().get("field2").toString(), equalTo("value2"));
    }

    public void testDLSIsAppliedBeforeFLS() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=text", "field2", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value1")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test", "type1", "2").setSource("field1", "value2", "field2", "value2")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        SearchResponse response = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test").setQuery(QueryBuilders.termQuery("field1", "value2"))
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "2");
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).getSourceAsMap().get("field1").toString(), equalTo("value2"));

        response = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test").setQuery(QueryBuilders.termQuery("field1", "value1"))
                .get();
        assertHitCount(response, 0);
    }

    public void testQueryCache() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .setSettings(Settings.builder().put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true))
                        .addMapping("type1", "field1", "type=text", "field2", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value2")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        // Both users have the same role query, but user3 has access to field2 and not field1, which should result in zero hits:
        int max = scaledRandomIntBetween(4, 32);
        for (int i = 0; i < max; i++) {
            SearchResponse response = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .prepareSearch("test")
                    .get();
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
            assertThat(response.getHits().getAt(0).getSourceAsMap().get("field1"), equalTo("value1"));
            response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                    .prepareSearch("test")
                    .get();
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
            assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
            assertThat(response.getHits().getAt(0).getSourceAsMap().get("field2"), equalTo("value2"));

            // this is a bit weird the document level permission (all docs with field2:value2) don't match with the field level
            // permissions (field1),
            // this results in document 2 being returned but no fields are visible:
            response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                    .prepareSearch("test")
                    .get();
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
            assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(0));

            // user4 has all roles
            response = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                    .prepareSearch("test")
                    .addSort("_uid", SortOrder.ASC)
                    .get();
            assertHitCount(response, 2);
            assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
            assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
            assertThat(response.getHits().getAt(0).getSourceAsMap().get("field1"), equalTo("value1"));
            assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
            assertThat(response.getHits().getAt(1).getSourceAsMap().size(), equalTo(1));
            assertThat(response.getHits().getAt(1).getSourceAsMap().get("field2"), equalTo("value2"));
        }
    }

}
