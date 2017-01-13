/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.fieldstats.FieldStatsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;

import java.util.Collections;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasChildQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

// The random usage of meta fields such as _timestamp add noise to the test, so disable random index templates:
@ESIntegTestCase.ClusterScope(randomDynamicTemplates = false)
public class FieldLevelSecurityTests extends SecurityIntegTestCase {

    protected static final SecuredString USERS_PASSWD = new SecuredString("change_me".toCharArray());
    protected static final String USERS_PASSWD_HASHED = new String(Hasher.BCRYPT.hash(new SecuredString("change_me".toCharArray())));

    @Override
    protected String configUsers() {
        return super.configUsers() +
                "user1:" + USERS_PASSWD_HASHED + "\n" +
                "user2:" + USERS_PASSWD_HASHED + "\n" +
                "user3:" + USERS_PASSWD_HASHED + "\n" +
                "user4:" + USERS_PASSWD_HASHED + "\n" +
                "user5:" + USERS_PASSWD_HASHED + "\n" +
                "user6:" + USERS_PASSWD_HASHED + "\n" +
                "user7:" + USERS_PASSWD_HASHED + "\n" +
                "user8:" + USERS_PASSWD_HASHED + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() +
                "role1:user1\n" +
                "role2:user1,user7,user8\n" +
                "role3:user2,user7,user8\n" +
                "role4:user3,user7\n" +
                "role5:user4,user7\n" +
                "role6:user5,user7\n" +
                "role7:user6";
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
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "      - names: '*'\n" +
                "        privileges: [ ALL ]\n" +
                "        field_security:\n" +
                "           grant: [ field1 ]\n" +
                "role3:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "      - names: '*'\n" +
                "        privileges: [ ALL ]\n" +
                "        field_security:\n" +
                "           grant: [ field2, query* ]\n" +
                "role4:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "     - names: '*'\n" +
                "       privileges: [ ALL ]\n" +
                "       field_security:\n" +
                "           grant: [ field1, field2]\n" +
                "role5:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "      - names: '*'\n" +
                "        privileges: [ ALL ]\n" +
                "        field_security:\n" +
                "           grant: [ ]\n" +
                "role6:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "     - names: '*'\n" +
                "       privileges: [ALL]\n" +
                "role7:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "      - names: '*'\n" +
                "        privileges: [ ALL ]\n" +
                "        field_security:\n" +
                "           grant: [ 'field*' ]\n";
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(XPackSettings.DLS_FLS_ENABLED.getKey(), true)
                .build();
    }

    public void testQuery() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2", "field3", "value3")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        // user1 has access to field1, so the query should match with the document:
        SearchResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field1", "value1"))
                .get();
        assertHitCount(response, 1);
        // user2 has no access to field1, so the query should not match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field1", "value1"))
                .get();
        assertHitCount(response, 0);
        // user3 has access to field1 and field2, so the query should match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field1", "value1"))
                .get();
        assertHitCount(response, 1);
        // user4 has access to no fields, so the query should not match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field1", "value1"))
                .get();
        assertHitCount(response, 0);
        // user5 has no field level security configured, so the query should match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field1", "value1"))
                .get();
        assertHitCount(response, 1);
        // user7 has roles with field level security configured and without field level security
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user7", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field1", "value1"))
                .get();
        assertHitCount(response, 1);
        // user8 has roles with field level security configured for field1 and field2
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field1", "value1"))
                .get();
        assertHitCount(response, 1);

        // user1 has no access to field1, so the query should not match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field2", "value2"))
                .get();
        assertHitCount(response, 0);
        // user2 has access to field1, so the query should match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field2", "value2"))
                .get();
        assertHitCount(response, 1);
        // user3 has access to field1 and field2, so the query should match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field2", "value2"))
                .get();
        assertHitCount(response, 1);
        // user4 has access to no fields, so the query should not match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field2", "value2"))
                .get();
        assertHitCount(response, 0);
        // user5 has no field level security configured, so the query should match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field2", "value2"))
                .get();
        assertHitCount(response, 1);
        // user7 has role with field level security and without field level security
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field2", "value2"))
                .get();
        assertHitCount(response, 1);
        // user8 has roles with field level security configured for field1 and field2
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field2", "value2"))
                .get();
        assertHitCount(response, 1);

        // user1 has access to field3, so the query should not match with the document:
        response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field3", "value3"))
                .get();
        assertHitCount(response, 0);
        // user2 has no access to field3, so the query should not match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field3", "value3"))
                .get();
        assertHitCount(response, 0);
        // user3 has access to field1 and field2 but not field3, so the query should not match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field3", "value3"))
                .get();
        assertHitCount(response, 0);
        // user4 has access to no fields, so the query should not match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field3", "value3"))
                .get();
        assertHitCount(response, 0);
        // user5 has no field level security configured, so the query should match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field3", "value3"))
                .get();
        assertHitCount(response, 1);
        // user7 has roles with field level security and without field level security
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field3", "value3"))
                .get();
        assertHitCount(response, 1);
        // user8 has roles with field level security configured for field1 and field2
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field3", "value3"))
                .get();
        assertHitCount(response, 0);
    }

    public void testGetApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text", "field2", "type=text", "field3", "type=text")
        );

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2", "field3", "value3")
                .get();

        boolean realtime = randomBoolean();
        // user1 is granted access to field1 only:
        GetResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareGet("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(1));
        assertThat(response.getSource().get("field1").toString(), equalTo("value1"));

        // user2 is granted access to field2 only:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareGet("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(1));
        assertThat(response.getSource().get("field2").toString(), equalTo("value2"));

        // user3 is granted access to field1 and field2:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareGet("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(2));
        assertThat(response.getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSource().get("field2").toString(), equalTo("value2"));

        // user4 is granted access to no fields, so the get response does say the doc exist, but no fields are returned:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareGet("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(0));

        // user5 has no field level security configured, so all fields are returned:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareGet("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(3));
        assertThat(response.getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSource().get("field2").toString(), equalTo("value2"));
        assertThat(response.getSource().get("field3").toString(), equalTo("value3"));

        // user6 has access to field*
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareGet("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(3));
        assertThat(response.getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSource().get("field2").toString(), equalTo("value2"));
        assertThat(response.getSource().get("field3").toString(), equalTo("value3"));

        // user7 has roles with field level security and without field level security
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user7", USERS_PASSWD)))
                .prepareGet("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(3));
        assertThat(response.getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSource().get("field2").toString(), equalTo("value2"));
        assertThat(response.getSource().get("field3").toString(), equalTo("value3"));

        // user8 has roles with field level security with access to field1 and field2
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
                .prepareGet("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(2));
        assertThat(response.getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSource().get("field2").toString(), equalTo("value2"));
    }

    public void testMGetApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2", "field3", "value3").get();

        boolean realtime = randomBoolean();
        // user1 is granted access to field1 only:
        MultiGetResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(1));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field1").toString(), equalTo("value1"));

        // user2 is granted access to field2 only:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(1));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));

        // user3 is granted access to field1 and field2:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(2));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));

        // user4 is granted access to no fields, so the get response does say the doc exist, but no fields are returned:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(0));

        // user5 has no field level security configured, so all fields are returned:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(3));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field3").toString(), equalTo("value3"));

        // user6 has access to field*
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(3));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field3").toString(), equalTo("value3"));

        // user7 has roles with field level security and without field level security
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user7", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(3));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field3").toString(), equalTo("value3"));

        // user8 has roles with field level security with access to field1 and field2
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(2));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));
    }

    public void testMSearchApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test1")
                .addMapping("type1", "field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        assertAcked(client().admin().indices().prepareCreate("test2")
                .addMapping("type1", "field1", "type=text", "field2", "type=text", "field3", "type=text")
        );

        client().prepareIndex("test1", "type1", "1")
                .setSource("field1", "value1", "field2", "value2", "field3", "value3").get();
        client().prepareIndex("test2", "type1", "1")
                .setSource("field1", "value1", "field2", "value2", "field3", "value3").get();
        client().admin().indices().prepareRefresh("test1", "test2").get();

        // user1 is granted access to field1 only
        MultiSearchResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareMultiSearch()
                .add(client().prepareSearch("test1").setTypes("type1").setQuery(QueryBuilders.matchAllQuery()))
                .add(client().prepareSearch("test2").setTypes("type1").setQuery(QueryBuilders.matchAllQuery()))
                .get();
        assertFalse(response.getResponses()[0].isFailure());
        assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits(), is(1L));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().size(), is(1));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().get("field1"), is("value1"));
        assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits(), is(1L));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().size(), is(1));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().get("field1"), is("value1"));

        // user2 is granted access to field2 only
        response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareMultiSearch()
                .add(client().prepareSearch("test1").setTypes("type1").setQuery(QueryBuilders.matchAllQuery()))
                .add(client().prepareSearch("test2").setTypes("type1").setQuery(QueryBuilders.matchAllQuery()))
                .get();
        assertFalse(response.getResponses()[0].isFailure());
        assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits(), is(1L));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().size(), is(1));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().get("field2"), is("value2"));
        assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits(), is(1L));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().size(), is(1));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().get("field2"), is("value2"));

        // user3 is granted access to field1 and field2
        response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareMultiSearch()
                .add(client().prepareSearch("test1").setTypes("type1").setQuery(QueryBuilders.matchAllQuery()))
                .add(client().prepareSearch("test2").setTypes("type1").setQuery(QueryBuilders.matchAllQuery()))
                .get();
        assertFalse(response.getResponses()[0].isFailure());
        assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits(), is(1L));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().size(), is(2));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().get("field1"), is("value1"));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().get("field2"), is("value2"));
        assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits(), is(1L));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().size(), is(2));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().get("field1"), is("value1"));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().get("field2"), is("value2"));

        // user4 is granted access to no fields, so the search response does say the doc exist, but no fields are returned
        response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareMultiSearch()
                .add(client().prepareSearch("test1").setTypes("type1").setQuery(QueryBuilders.matchAllQuery()))
                .add(client().prepareSearch("test2").setTypes("type1").setQuery(QueryBuilders.matchAllQuery()))
                .get();
        assertFalse(response.getResponses()[0].isFailure());
        assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits(), is(1L));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().size(), is(0));
        assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits(), is(1L));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().size(), is(0));

        // user5 has no field level security configured, so all fields are returned
        response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareMultiSearch()
                .add(client().prepareSearch("test1").setTypes("type1").setQuery(QueryBuilders.matchAllQuery()))
                .add(client().prepareSearch("test2").setTypes("type1").setQuery(QueryBuilders.matchAllQuery()))
                .get();
        assertFalse(response.getResponses()[0].isFailure());
        assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits(), is(1L));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().size(), is(3));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().get("field1"), is("value1"));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().get("field2"), is("value2"));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().get("field3"), is("value3"));
        assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits(), is(1L));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().size(), is(3));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().get("field1"), is("value1"));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().get("field2"), is("value2"));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().get("field3"), is("value3"));

        // user6 has access to field*
        response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareMultiSearch()
                .add(client().prepareSearch("test1").setTypes("type1").setQuery(QueryBuilders.matchAllQuery()))
                .add(client().prepareSearch("test2").setTypes("type1").setQuery(QueryBuilders.matchAllQuery()))
                .get();
        assertFalse(response.getResponses()[0].isFailure());
        assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits(), is(1L));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().size(), is(3));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().get("field1"), is("value1"));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().get("field2"), is("value2"));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().get("field3"), is("value3"));
        assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits(), is(1L));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().size(), is(3));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().get("field1"), is("value1"));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().get("field2"), is("value2"));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().get("field3"), is("value3"));

        // user7 has roles with field level security and without field level security
        response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user7", USERS_PASSWD)))
                .prepareMultiSearch()
                .add(client().prepareSearch("test1").setTypes("type1").setQuery(QueryBuilders.matchAllQuery()))
                .add(client().prepareSearch("test2").setTypes("type1").setQuery(QueryBuilders.matchAllQuery()))
                .get();
        assertFalse(response.getResponses()[0].isFailure());
        assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits(), is(1L));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().size(), is(3));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().get("field1"), is("value1"));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().get("field2"), is("value2"));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().get("field3"), is("value3"));
        assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits(), is(1L));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().size(), is(3));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().get("field1"), is("value1"));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().get("field2"), is("value2"));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().get("field3"), is("value3"));

        // user8 has roles with field level security with access to field1 and field2
        response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
                .prepareMultiSearch()
                .add(client().prepareSearch("test1").setTypes("type1").setQuery(QueryBuilders.matchAllQuery()))
                .add(client().prepareSearch("test2").setTypes("type1").setQuery(QueryBuilders.matchAllQuery()))
                .get();
        assertFalse(response.getResponses()[0].isFailure());
        assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits(), is(1L));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().size(), is(2));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().get("field1"), is("value1"));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSource().get("field2"), is("value2"));
        assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits(), is(1L));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().size(), is(2));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().get("field1"), is("value1"));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSource().get("field2"), is("value2"));
    }

    public void testFieldStatsApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2", "field3", "value3")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        // user1 is granted access to field1 only:
        FieldStatsResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareFieldStats()
                .setFields("field1", "field2", "field3")
                .get();
        assertThat(response.getAllFieldStats().size(), equalTo(1));
        assertThat(response.getAllFieldStats().get("field1").getDocCount(), equalTo(1L));

        // user2 is granted access to field2 only:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareFieldStats()
                .setFields("field1", "field2", "field3")
                .get();
        assertThat(response.getAllFieldStats().size(), equalTo(1));
        assertThat(response.getAllFieldStats().get("field2").getDocCount(), equalTo(1L));

        // user3 is granted access to field1 and field2:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareFieldStats()
                .setFields("field1", "field2", "field3")
                .get();
        assertThat(response.getAllFieldStats().size(), equalTo(2));
        assertThat(response.getAllFieldStats().get("field1").getDocCount(), equalTo(1L));
        assertThat(response.getAllFieldStats().get("field2").getDocCount(), equalTo(1L));

        // user4 is granted access to no fields:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareFieldStats()
                .setFields("field1", "field2")
                .get();
        assertThat(response.getAllFieldStats().size(), equalTo(0));

        // user5 has no field level security configured:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareFieldStats()
                .setFields("field1", "field2", "field3")
                .get();
        assertThat(response.getAllFieldStats().size(), equalTo(3));
        assertThat(response.getAllFieldStats().get("field1").getDocCount(), equalTo(1L));
        assertThat(response.getAllFieldStats().get("field2").getDocCount(), equalTo(1L));
        assertThat(response.getAllFieldStats().get("field3").getDocCount(), equalTo(1L));

        // user6 has field level security configured for field*:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareFieldStats()
                .setFields("field1", "field2", "field3")
                .get();
        assertThat(response.getAllFieldStats().size(), equalTo(3));
        assertThat(response.getAllFieldStats().get("field1").getDocCount(), equalTo(1L));
        assertThat(response.getAllFieldStats().get("field2").getDocCount(), equalTo(1L));
        assertThat(response.getAllFieldStats().get("field3").getDocCount(), equalTo(1L));

        // user7 has no field level security configured (roles with and without field level security):
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user7", USERS_PASSWD)))
                .prepareFieldStats()
                .setFields("field1", "field2", "field3")
                .get();
        assertThat(response.getAllFieldStats().size(), equalTo(3));
        assertThat(response.getAllFieldStats().get("field1").getDocCount(), equalTo(1L));
        assertThat(response.getAllFieldStats().get("field2").getDocCount(), equalTo(1L));
        assertThat(response.getAllFieldStats().get("field3").getDocCount(), equalTo(1L));

        // user8 has field level security configured for field1 and field2 (multiple roles):
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
                .prepareFieldStats()
                .setFields("field1", "field2", "field3")
                .get();
        assertThat(response.getAllFieldStats().size(), equalTo(2));
        assertThat(response.getAllFieldStats().get("field1").getDocCount(), equalTo(1L));
        assertThat(response.getAllFieldStats().get("field2").getDocCount(), equalTo(1L));
    }

    public void testScroll() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true))
                .addMapping("type1", "field1", "type=text", "field2", "type=text", "field3", "type=text")
        );

        final int numDocs = scaledRandomIntBetween(2, 10);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test", "type1", String.valueOf(i))
                    .setSource("field1", "value1", "field2", "value2", "field3", "value3")
                    .get();
        }
        refresh("test");

        SearchResponse response = null;
        try {
            response = client()
                    .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .prepareSearch("test")
                    .setScroll(TimeValue.timeValueMinutes(1L))
                    .setSize(1)
                    .setQuery(constantScoreQuery(termQuery("field1", "value1")))
                    .setFetchSource(true)
                    .get();

            do {
                assertThat(response.getHits().getTotalHits(), is((long) numDocs));
                assertThat(response.getHits().getHits().length, is(1));
                assertThat(response.getHits().getAt(0).getSource().size(), is(1));
                assertThat(response.getHits().getAt(0).getSource().get("field1"), is("value1"));

                if (response.getScrollId() == null) {
                    break;
                }

                response = client()
                        .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                        .prepareSearchScroll(response.getScrollId())
                        .setScroll(TimeValue.timeValueMinutes(1L))
                        .get();
            } while (response.getHits().getHits().length > 0);

        } finally {
            if (response != null) {
                String scrollId = response.getScrollId();
                if (scrollId != null) {
                    client().prepareClearScroll().addScrollId(scrollId).get();
                }
            }
        }
    }

    public void testQueryCache() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .setSettings(Settings.builder().put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true))
                        .addMapping("type1", "field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2", "field3", "value3")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        int max = scaledRandomIntBetween(4, 32);
        for (int i = 0; i < max; i++) {
            SearchResponse response = client()
                    .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .prepareSearch("test")
                    .setQuery(constantScoreQuery(termQuery("field1", "value1")))
                    .get();
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getSource().size(), is(1));
            assertThat(response.getHits().getAt(0).getSource().get("field1"), is("value1"));
            response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                    .prepareSearch("test")
                    .setQuery(constantScoreQuery(termQuery("field1", "value1")))
                    .get();
            assertHitCount(response, 0);
            String multipleFieldsUser = randomFrom("user5", "user6", "user7");
            response = client()
                    .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue(multipleFieldsUser, USERS_PASSWD)))
                    .prepareSearch("test")
                    .setQuery(constantScoreQuery(termQuery("field1", "value1")))
                    .get();
            assertHitCount(response, 1);
            assertThat(response.getHits().getAt(0).getSource().size(), is(3));
            assertThat(response.getHits().getAt(0).getSource().get("field1"), is("value1"));
            assertThat(response.getHits().getAt(0).getSource().get("field2"), is("value2"));
            assertThat(response.getHits().getAt(0).getSource().get("field3"), is("value3"));
        }
    }

    public void testRequestCache() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .setSettings(Settings.builder().put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true))
                        .addMapping("type1", "field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        int max = scaledRandomIntBetween(4, 32);
        for (int i = 0; i < max; i++) {
            Boolean requestCache = randomFrom(true, null);
            SearchResponse response = client()
                    .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .prepareSearch("test")
                    .setSize(0)
                    .setQuery(termQuery("field1", "value1"))
                    .setRequestCache(requestCache)
                    .get();
            assertNoFailures(response);
            assertHitCount(response, 1);
            response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                    .prepareSearch("test")
                    .setSize(0)
                    .setQuery(termQuery("field1", "value1"))
                    .setRequestCache(requestCache)
                    .get();
            assertNoFailures(response);
            assertHitCount(response, 0);
            String multipleFieldsUser = randomFrom("user5", "user6", "user7");
            response = client()
                    .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue(multipleFieldsUser, USERS_PASSWD)))
                    .prepareSearch("test")
                    .setSize(0)
                    .setQuery(termQuery("field1", "value1"))
                    .setRequestCache(requestCache)
                    .get();
            assertNoFailures(response);
            assertHitCount(response, 1);
        }
    }

    public void testFields() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text,store=true", "field2", "type=text,store=true",
                                "field3", "type=text,store=true")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2", "field3", "value3")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        // user1 is granted access to field1 only:
        SearchResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .addStoredField("field1")
                .addStoredField("field2")
                .addStoredField("field3")
                .get();
        assertThat(response.getHits().getAt(0).fields().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).fields().get("field1").<String>getValue(), equalTo("value1"));

        // user2 is granted access to field2 only:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .addStoredField("field1")
                .addStoredField("field2")
                .addStoredField("field3")
                .get();
        assertThat(response.getHits().getAt(0).fields().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).fields().get("field2").<String>getValue(), equalTo("value2"));

        // user3 is granted access to field1 and field2:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .addStoredField("field1")
                .addStoredField("field2")
                .addStoredField("field3")
                .get();
        assertThat(response.getHits().getAt(0).fields().size(), equalTo(2));
        assertThat(response.getHits().getAt(0).fields().get("field1").<String>getValue(), equalTo("value1"));
        assertThat(response.getHits().getAt(0).fields().get("field2").<String>getValue(), equalTo("value2"));

        // user4 is granted access to no fields:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test")
                .addStoredField("field1")
                .addStoredField("field2")
                .addStoredField("field3")
                .get();
        assertThat(response.getHits().getAt(0).fields().size(), equalTo(0));

        // user5 has no field level security configured:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test")
                .addStoredField("field1")
                .addStoredField("field2")
                .addStoredField("field3")
                .get();
        assertThat(response.getHits().getAt(0).fields().size(), equalTo(3));
        assertThat(response.getHits().getAt(0).fields().get("field1").<String>getValue(), equalTo("value1"));
        assertThat(response.getHits().getAt(0).fields().get("field2").<String>getValue(), equalTo("value2"));
        assertThat(response.getHits().getAt(0).fields().get("field3").<String>getValue(), equalTo("value3"));

        // user6 has field level security configured with access to field*:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareSearch("test")
                .addStoredField("field1")
                .addStoredField("field2")
                .addStoredField("field3")
                .get();
        assertThat(response.getHits().getAt(0).fields().size(), equalTo(3));
        assertThat(response.getHits().getAt(0).fields().get("field1").<String>getValue(), equalTo("value1"));
        assertThat(response.getHits().getAt(0).fields().get("field2").<String>getValue(), equalTo("value2"));
        assertThat(response.getHits().getAt(0).fields().get("field3").<String>getValue(), equalTo("value3"));

        // user7 has access to all fields due to a mix of roles without field level security and with:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user7", USERS_PASSWD)))
                .prepareSearch("test")
                .addStoredField("field1")
                .addStoredField("field2")
                .addStoredField("field3")
                .get();
        assertThat(response.getHits().getAt(0).fields().size(), equalTo(3));
        assertThat(response.getHits().getAt(0).fields().get("field1").<String>getValue(), equalTo("value1"));
        assertThat(response.getHits().getAt(0).fields().get("field2").<String>getValue(), equalTo("value2"));
        assertThat(response.getHits().getAt(0).fields().get("field3").<String>getValue(), equalTo("value3"));

        // user8 has field level security configured with access to field1 and field2:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
                .prepareSearch("test")
                .addStoredField("field1")
                .addStoredField("field2")
                .addStoredField("field3")
                .get();
        assertThat(response.getHits().getAt(0).fields().size(), equalTo(2));
        assertThat(response.getHits().getAt(0).fields().get("field1").<String>getValue(), equalTo("value1"));
        assertThat(response.getHits().getAt(0).fields().get("field2").<String>getValue(), equalTo("value2"));
    }

    public void testSource() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2", "field3", "value3")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        // user1 is granted access to field1 only:
        SearchResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .get();
        assertThat(response.getHits().getAt(0).sourceAsMap().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field1").toString(), equalTo("value1"));

        // user2 is granted access to field2 only:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .get();
        assertThat(response.getHits().getAt(0).sourceAsMap().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field2").toString(), equalTo("value2"));

        // user3 is granted access to field1 and field2:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .get();
        assertThat(response.getHits().getAt(0).sourceAsMap().size(), equalTo(2));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field2").toString(), equalTo("value2"));

        // user4 is granted access to no fields:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test")
                .get();
        assertThat(response.getHits().getAt(0).sourceAsMap().size(), equalTo(0));

        // user5 has no field level security configured:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch("test")
                .get();
        assertThat(response.getHits().getAt(0).sourceAsMap().size(), equalTo(3));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field2").toString(), equalTo("value2"));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field3").toString(), equalTo("value3"));

        // user6 has field level security configured with access to field*:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareSearch("test")
                .get();
        assertThat(response.getHits().getAt(0).sourceAsMap().size(), equalTo(3));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field2").toString(), equalTo("value2"));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field3").toString(), equalTo("value3"));

        // user7 has access to all fields
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user7", USERS_PASSWD)))
                .prepareSearch("test")
                .get();
        assertThat(response.getHits().getAt(0).sourceAsMap().size(), equalTo(3));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field2").toString(), equalTo("value2"));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field3").toString(), equalTo("value3"));

        // user8 has field level security configured with access to field1 and field2:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
                .prepareSearch("test")
                .get();
        assertThat(response.getHits().getAt(0).sourceAsMap().size(), equalTo(2));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field2").toString(), equalTo("value2"));
    }

    public void testSort() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=long", "field2", "type=long")
        );

        client().prepareIndex("test", "type1", "1").setSource("field1", 1d, "field2", 2d)
                .setRefreshPolicy(IMMEDIATE)
                .get();

        // user1 is granted to use field1, so it is included in the sort_values
        SearchResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .addSort("field1", SortOrder.ASC)
                .get();
        assertThat(response.getHits().getAt(0).sortValues()[0], equalTo(1L));

        // user2 is not granted to use field1, so the default missing sort value is included
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .addSort("field1", SortOrder.ASC)
                .get();
        assertThat(response.getHits().getAt(0).sortValues()[0], equalTo(Long.MAX_VALUE));

        // user1 is not granted to use field2, so the default missing sort value is included
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .addSort("field2", SortOrder.ASC)
                .get();
        assertThat(response.getHits().getAt(0).sortValues()[0], equalTo(Long.MAX_VALUE));

        // user2 is granted to use field2, so it is included in the sort_values
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .addSort("field2", SortOrder.ASC)
                .get();
        assertThat(response.getHits().getAt(0).sortValues()[0], equalTo(2L));
    }

    public void testAggs() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text,fielddata=true", "field2", "type=text,fielddata=true")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        // user1 is authorized to use field1, so buckets are include for a term agg on field1
        SearchResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .addAggregation(AggregationBuilders.terms("_name").field("field1"))
                .get();
        assertThat(((Terms) response.getAggregations().get("_name")).getBucketByKey("value1").getDocCount(), equalTo(1L));

        // user2 is not authorized to use field1, so no buckets are include for a term agg on field1
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .addAggregation(AggregationBuilders.terms("_name").field("field1"))
                .get();
        assertThat(((Terms) response.getAggregations().get("_name")).getBucketByKey("value1"), nullValue());

        // user1 is not authorized to use field2, so no buckets are include for a term agg on field2
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .addAggregation(AggregationBuilders.terms("_name").field("field2"))
                .get();
        assertThat(((Terms) response.getAggregations().get("_name")).getBucketByKey("value2"), nullValue());

        // user2 is authorized to use field2, so buckets are include for a term agg on field2
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .addAggregation(AggregationBuilders.terms("_name").field("field2"))
                .get();
        assertThat(((Terms) response.getAggregations().get("_name")).getBucketByKey("value2").getDocCount(), equalTo(1L));
    }

    public void testTVApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text,term_vector=with_positions_offsets_payloads",
                                "field2", "type=text,term_vector=with_positions_offsets_payloads",
                                "field3", "type=text,term_vector=with_positions_offsets_payloads")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2", "field3", "value3")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        boolean realtime = randomBoolean();
        TermVectorsResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareTermVectors("test", "type1", "1")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(1));
        assertThat(response.getFields().terms("field1").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareTermVectors("test", "type1", "1")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(1));
        assertThat(response.getFields().terms("field2").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareTermVectors("test", "type1", "1")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(2));
        assertThat(response.getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getFields().terms("field2").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareTermVectors("test", "type1", "1")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(0));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareTermVectors("test", "type1", "1")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(3));
        assertThat(response.getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getFields().terms("field2").size(), equalTo(1L));
        assertThat(response.getFields().terms("field3").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareTermVectors("test", "type1", "1")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(3));
        assertThat(response.getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getFields().terms("field2").size(), equalTo(1L));
        assertThat(response.getFields().terms("field3").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user7", USERS_PASSWD)))
                .prepareTermVectors("test", "type1", "1")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(3));
        assertThat(response.getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getFields().terms("field2").size(), equalTo(1L));
        assertThat(response.getFields().terms("field3").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
                .prepareTermVectors("test", "type1", "1")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(2));
        assertThat(response.getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getFields().terms("field2").size(), equalTo(1L));
    }

    public void testMTVApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text,term_vector=with_positions_offsets_payloads",
                                "field2", "type=text,term_vector=with_positions_offsets_payloads",
                                "field3", "type=text,term_vector=with_positions_offsets_payloads")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2", "field3", "value3")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        boolean realtime = randomBoolean();
        MultiTermVectorsResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "type1", "1").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(1));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field1").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "type1", "1").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(1));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field2").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "type1", "1").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(2));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field2").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "type1", "1").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(0));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "type1", "1").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(3));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field2").size(), equalTo(1L));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field3").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "type1", "1").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(3));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field2").size(), equalTo(1L));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field3").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user7", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "type1", "1").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(3));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field2").size(), equalTo(1L));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field3").size(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user8", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "type1", "1").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(2));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field1").size(), equalTo(1L));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field2").size(), equalTo(1L));
    }

    public void testParentChild() {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent"));
        ensureGreen();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("{}").get();
        client().prepareIndex("test", "child", "c1").setSource("field1", "red").setParent("p1").get();
        client().prepareIndex("test", "child", "c2").setSource("field1", "yellow").setParent("p1").get();
        refresh();

        SearchResponse searchResponse = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasChildQuery("child", termQuery("field1", "yellow"), ScoreMode.None))
                .get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1L));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasChildQuery("child", termQuery("field1", "yellow"), ScoreMode.None))
                .get();
        assertHitCount(searchResponse, 0L);
    }

    public void testUpdateApiIsBlocked() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type", "field1", "type=text", "field2", "type=text")
        );
        client().prepareIndex("test", "type", "1")
                .setSource("field1", "value1", "field2", "value1")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        // With field level security enabled the update is not allowed:
        try {
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .prepareUpdate("test", "type", "1").setDoc("field2", "value2")
                    .get();
            fail("failed, because update request shouldn't be allowed if field level security is enabled");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(e.getMessage(), equalTo("Can't execute an update request if field or document level security is enabled"));
        }
        assertThat(client().prepareGet("test", "type", "1").get().getSource().get("field2").toString(), equalTo("value1"));

        // With no field level security enabled the update is allowed:
        client().prepareUpdate("test", "type", "1").setDoc("field2", "value2")
                .get();
        assertThat(client().prepareGet("test", "type", "1").get().getSource().get("field2").toString(), equalTo("value2"));

        // With field level security enabled the update in bulk is not allowed:
        BulkResponse bulkResponse = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue
                ("user1", USERS_PASSWD)))
                .prepareBulk()
                .add(new UpdateRequest("test", "type", "1").doc("field2", "value3"))
                .get();
        assertEquals(1, bulkResponse.getItems().length);
        BulkItemResponse bulkItem = bulkResponse.getItems()[0];
        assertTrue(bulkItem.isFailed());
        assertThat(bulkItem.getFailure().getCause(), instanceOf(ElasticsearchSecurityException.class));
        ElasticsearchSecurityException securityException = (ElasticsearchSecurityException) bulkItem.getFailure().getCause();
        assertThat(securityException.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(securityException.getMessage(),
                equalTo("Can't execute a bulk request with update requests embedded if field or document level security is enabled"));

        assertThat(client().prepareGet("test", "type", "1").get().getSource().get("field2").toString(), equalTo("value2"));

        client().prepareBulk()
                .add(new UpdateRequest("test", "type", "1").doc("field2", "value3"))
                .get();
        assertThat(client().prepareGet("test", "type", "1").get().getSource().get("field2").toString(), equalTo("value3"));
    }

    public void testQuery_withRoleWithFieldWildcards() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text", "field2", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        // user6 has access to all fields, so the query should match with the document:
        SearchResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field1", "value1"))
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).sourceAsMap().size(), equalTo(2));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field2").toString(), equalTo("value2"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(matchQuery("field2", "value2"))
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).sourceAsMap().size(), equalTo(2));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field2").toString(), equalTo("value2"));
    }

    public void testExistQuery() {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type1", "field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2", "field3", "value3")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        // user1 has access to field1, so the query should match with the document:
        SearchResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(existsQuery("field1"))
                .get();
        assertHitCount(response, 1);
        // user1 has no access to field2, so the query should not match with the document:
        response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(existsQuery("field2"))
                .get();
        assertHitCount(response, 0);
        // user2 has no access to field1, so the query should not match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(existsQuery("field1"))
                .get();
        assertHitCount(response, 0);
        // user2 has access to field2, so the query should match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(existsQuery("field2"))
                .get();
        assertHitCount(response, 1);
        // user3 has access to field1 and field2, so the query should match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(existsQuery("field1"))
                .get();
        assertHitCount(response, 1);
        // user3 has access to field1 and field2, so the query should match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(existsQuery("field2"))
                .get();
        assertHitCount(response, 1);
        // user4 has access to no fields, so the query should not match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(existsQuery("field1"))
                .get();
        assertHitCount(response, 0);
        // user4 has access to no fields, so the query should not match with the document:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(existsQuery("field2"))
                .get();
        assertHitCount(response, 0);
    }

}
