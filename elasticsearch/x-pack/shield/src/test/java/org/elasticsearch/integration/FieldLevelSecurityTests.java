/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.fieldstats.FieldStatsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.action.percolate.PercolateSourceBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.indices.cache.request.IndicesRequestCache;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ShieldIntegTestCase;

import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasChildQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

// The random usage of meta fields such as _timestamp add noice to the test, so disable random index templates:
@ESIntegTestCase.ClusterScope(randomDynamicTemplates = false)
public class FieldLevelSecurityTests extends ShieldIntegTestCase {

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
                "user6:" + USERS_PASSWD_HASHED + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() +
                "role1:user1\n" +
                "role2:user2\n" +
                "role3:user3\n" +
                "role4:user4\n" +
                "role5:user5\n" +
                "role5:user6\n";
    }
    @Override
    protected String configRoles() {
        return super.configRoles() +
                "\nrole1:\n" +
                "  cluster: all\n" +
                "  indices:\n" +
                "     '*':\n" +
                "        privileges: ALL\n" +
                "        fields: field1\n" +
                "role2:\n" +
                "  cluster: all\n" +
                "  indices:\n" +
                "     '*':\n" +
                "        privileges: ALL\n" +
                "        fields: field2\n" +
                "role3:\n" +
                "  cluster: all\n" +
                "  indices:\n" +
                "     '*':\n" +
                "        privileges: ALL\n" +
                "        fields: \n" +
                "           - field1\n" +
                "           - field2\n" +
                "role4:\n" +
                "  cluster: all\n" +
                "  indices:\n" +
                "     '*':\n" +
                "        privileges: ALL\n" +
                "        fields:\n" +
                "role5:\n" +
                "  cluster: all\n" +
                "  indices:\n" +
                "     '*': ALL\n" +
                "role6:\n" +
                "  cluster: all\n" +
                "  indices:\n" +
                "        privileges: ALL\n" +
                "        fields: 'field*'\n";
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(ShieldPlugin.DLS_FLS_ENABLED_SETTING, true)
                .build();
    }

    public void testQuery() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string", "field2", "type=string")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        // user1 has access to field1, so the query should match with the document:
        SearchResponse response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                .setQuery(matchQuery("field1", "value1"))
                .get();
        assertHitCount(response, 1);
        // user2 has no access to field1, so the query should not match with the document:
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                .setQuery(matchQuery("field1", "value1"))
                .get();
        assertHitCount(response, 0);
        // user3 has access to field1 and field2, so the query should match with the document:
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD))
                .setQuery(matchQuery("field1", "value1"))
                .get();
        assertHitCount(response, 1);
        // user4 has access to no fields, so the query should not match with the document:
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD))
                .setQuery(matchQuery("field1", "value1"))
                .get();
        assertHitCount(response, 0);
        // user5 has no field level security configured, so the query should match with the document:
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD))
                .setQuery(matchQuery("field1", "value1"))
                .get();
        assertHitCount(response, 1);

        // user1 has no access to field1, so the query should not match with the document:
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                .setQuery(matchQuery("field2", "value2"))
                .get();
        assertHitCount(response, 0);
        // user2 has access to field1, so the query should match with the document:
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                .setQuery(matchQuery("field2", "value2"))
                .get();
        assertHitCount(response, 1);
        // user3 has access to field1 and field2, so the query should match with the document:
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD))
                .setQuery(matchQuery("field2", "value2"))
                .get();
        assertHitCount(response, 1);
        // user4 has access to no fields, so the query should not match with the document:
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD))
                .setQuery(matchQuery("field2", "value2"))
                .get();
        assertHitCount(response, 0);
        // user5 has no field level security configured, so the query should match with the document:
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD))
                .setQuery(matchQuery("field2", "value2"))
                .get();
        assertHitCount(response, 1);
    }

    public void testGetApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string", "field2", "type=string")
        );

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .get();

        Boolean realtime = randomFrom(true, false, null);
        // user1 is granted access to field1 only:
        GetResponse response = client().prepareGet("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(1));
        assertThat(response.getSource().get("field1").toString(), equalTo("value1"));

        // user2 is granted access to field2 only:
        response = client().prepareGet("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(1));
        assertThat(response.getSource().get("field2").toString(), equalTo("value2"));

        // user3 is granted access to field1 and field2:
        response = client().prepareGet("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD))
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(2));
        assertThat(response.getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSource().get("field2").toString(), equalTo("value2"));

        // user4 is granted access to no fields, so the get response does say the doc exist, but no fields are returned:
        response = client().prepareGet("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD))
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(0));

        // user5 has no field level security configured, so all fields are returned:
        response = client().prepareGet("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD))
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getSource().size(), equalTo(2));
        assertThat(response.getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSource().get("field2").toString(), equalTo("value2"));
    }

    public void testMGetApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string", "field2", "type=string")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .get();

        Boolean realtime = randomFrom(true, false, null);
        // user1 is granted access to field1 only:
        MultiGetResponse response = client().prepareMultiGet()
                .add("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(1));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field1").toString(), equalTo("value1"));

        // user2 is granted access to field2 only:
        response = client().prepareMultiGet()
                .add("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(1));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));

        // user3 is granted access to field1 and field2:
        response = client().prepareMultiGet()
                .add("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD))
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(2));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));

        // user4 is granted access to no fields, so the get response does say the doc exist, but no fields are returned:
        response = client().prepareMultiGet()
                .add("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD))
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(0));

        // user5 has no field level security configured, so all fields are returned:
        response = client().prepareMultiGet()
                .add("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD))
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getSource().size(), equalTo(2));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field1").toString(), equalTo("value1"));
        assertThat(response.getResponses()[0].getResponse().getSource().get("field2").toString(), equalTo("value2"));
    }

    public void testFieldStatsApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string", "field2", "type=string")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        // user1 is granted access to field1 only:
        FieldStatsResponse response = client().prepareFieldStats()
                .setFields("field1", "field2")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                .get();
        assertThat(response.getAllFieldStats().size(), equalTo(1));
        assertThat(response.getAllFieldStats().get("field1").getDocCount(), equalTo(1l));

        // user2 is granted access to field2 only:
        response = client().prepareFieldStats()
                .setFields("field1", "field2")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                .get();
        assertThat(response.getAllFieldStats().size(), equalTo(1));
        assertThat(response.getAllFieldStats().get("field2").getDocCount(), equalTo(1l));

        // user3 is granted access to field1 and field2:
        response = client().prepareFieldStats()
                .setFields("field1", "field2")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD))
                .get();
        assertThat(response.getAllFieldStats().size(), equalTo(2));
        assertThat(response.getAllFieldStats().get("field1").getDocCount(), equalTo(1l));
        assertThat(response.getAllFieldStats().get("field2").getDocCount(), equalTo(1l));

        // user4 is granted access to no fields:
        response = client().prepareFieldStats()
                .setFields("field1", "field2")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD))
                .get();
        assertThat(response.getAllFieldStats().size(), equalTo(0));

        // user5 has no field level security configured:
        response = client().prepareFieldStats()
                .setFields("field1", "field2")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD))
                .get();
        assertThat(response.getAllFieldStats().size(), equalTo(2));
        assertThat(response.getAllFieldStats().get("field1").getDocCount(), equalTo(1l));
        assertThat(response.getAllFieldStats().get("field2").getDocCount(), equalTo(1l));
    }

    public void testQueryCache() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .setSettings(Settings.builder().put(IndexModule.QUERY_CACHE_EVERYTHING, true))
                        .addMapping("type1", "field1", "type=string", "field2", "type=string")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        int max = scaledRandomIntBetween(4, 32);
        for (int i = 0; i < max; i++) {
            SearchResponse response = client().prepareSearch("test")
                    .setQuery(constantScoreQuery(termQuery("field1", "value1")))
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                    .get();
            assertHitCount(response, 1);
            response = client().prepareSearch("test")
                    .setQuery(constantScoreQuery(termQuery("field1", "value1")))
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                    .get();
            assertHitCount(response, 0);
        }
    }

    public void testRequestCache() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .setSettings(Settings.builder().put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED, true))
                        .addMapping("type1", "field1", "type=string", "field2", "type=string")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        int max = scaledRandomIntBetween(4, 32);
        for (int i = 0; i < max; i++) {
            Boolean requestCache = randomFrom(true, null);
            SearchResponse response = client().prepareSearch("test")
                    .setSize(0)
                    .setQuery(termQuery("field1", "value1"))
                    .setRequestCache(requestCache)
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                    .get();
            assertNoFailures(response);
            assertHitCount(response, 1);
            response = client().prepareSearch("test")
                    .setSize(0)
                    .setQuery(termQuery("field1", "value1"))
                    .setRequestCache(requestCache)
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                    .get();
            assertNoFailures(response);
            assertHitCount(response, 0);
        }
    }

    public void testFields() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string,store=yes", "field2", "type=string,store=yes")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        // user1 is granted access to field1 only:
        SearchResponse response = client().prepareSearch("test")
                .addField("field1")
                .addField("field2")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                .get();
        assertThat(response.getHits().getAt(0).fields().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).fields().get("field1").<String>getValue(), equalTo("value1"));

        // user2 is granted access to field2 only:
        response = client().prepareSearch("test")
                .addField("field1")
                .addField("field2")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                .get();
        assertThat(response.getHits().getAt(0).fields().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).fields().get("field2").<String>getValue(), equalTo("value2"));

        // user3 is granted access to field1 and field2:
        response = client().prepareSearch("test")
                .addField("field1")
                .addField("field2")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD))
                .get();
        assertThat(response.getHits().getAt(0).fields().size(), equalTo(2));
        assertThat(response.getHits().getAt(0).fields().get("field1").<String>getValue(), equalTo("value1"));
        assertThat(response.getHits().getAt(0).fields().get("field2").<String>getValue(), equalTo("value2"));

        // user4 is granted access to no fields:
        response = client().prepareSearch("test")
                .addField("field1")
                .addField("field2")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD))
                .get();
        assertThat(response.getHits().getAt(0).fields().size(), equalTo(0));

        // user5 has no field level security configured:
        response = client().prepareSearch("test")
                .addField("field1")
                .addField("field2")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD))
                .get();
        assertThat(response.getHits().getAt(0).fields().size(), equalTo(2));
        assertThat(response.getHits().getAt(0).fields().get("field1").<String>getValue(), equalTo("value1"));
        assertThat(response.getHits().getAt(0).fields().get("field2").<String>getValue(), equalTo("value2"));
    }

    public void testSource() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string", "field2", "type=string")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        // user1 is granted access to field1 only:
        SearchResponse response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                .get();
        assertThat(response.getHits().getAt(0).sourceAsMap().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field1").toString(), equalTo("value1"));

        // user2 is granted access to field2 only:
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                .get();
        assertThat(response.getHits().getAt(0).sourceAsMap().size(), equalTo(1));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field2").toString(), equalTo("value2"));

        // user3 is granted access to field1 and field2:
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD))
                .get();
        assertThat(response.getHits().getAt(0).sourceAsMap().size(), equalTo(2));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field2").toString(), equalTo("value2"));

        // user4 is granted access to no fields:
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD))
                .get();
        assertThat(response.getHits().getAt(0).sourceAsMap().size(), equalTo(0));

        // user5 has no field level security configured:
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD))
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
                .setRefresh(true)
                .get();

        // user1 is granted to use field1, so it is included in the sort_values
        SearchResponse response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                .addSort("field1", SortOrder.ASC)
                .get();
        assertThat((Long) response.getHits().getAt(0).sortValues()[0], equalTo(1l));

        // user2 is not granted to use field1, so the default missing sort value is included
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                .addSort("field1", SortOrder.ASC)
                .get();
        assertThat((Long) response.getHits().getAt(0).sortValues()[0], equalTo(Long.MAX_VALUE));

        // user1 is not granted to use field2, so the default missing sort value is included
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                .addSort("field2", SortOrder.ASC)
                .get();
        assertThat((Long) response.getHits().getAt(0).sortValues()[0], equalTo(Long.MAX_VALUE));

        // user2 is granted to use field2, so it is included in the sort_values
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                .addSort("field2", SortOrder.ASC)
                .get();
        assertThat((Long) response.getHits().getAt(0).sortValues()[0], equalTo(2l));
    }

    public void testAggs() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string", "field2", "type=string")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        // user1 is authorized to use field1, so buckets are include for a term agg on field1
        SearchResponse response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                .addAggregation(AggregationBuilders.terms("_name").field("field1"))
                .get();
        assertThat(((Terms) response.getAggregations().get("_name")).getBucketByKey("value1").getDocCount(), equalTo(1l));

        // user2 is not authorized to use field1, so no buckets are include for a term agg on field1
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                .addAggregation(AggregationBuilders.terms("_name").field("field1"))
                .get();
        assertThat(((Terms) response.getAggregations().get("_name")).getBucketByKey("value1"), nullValue());

        // user1 is not authorized to use field2, so no buckets are include for a term agg on field2
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                .addAggregation(AggregationBuilders.terms("_name").field("field2"))
                .get();
        assertThat(((Terms) response.getAggregations().get("_name")).getBucketByKey("value2"), nullValue());

        // user2 is authorized to use field2, so buckets are include for a term agg on field2
        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                .addAggregation(AggregationBuilders.terms("_name").field("field2"))
                .get();
        assertThat(((Terms) response.getAggregations().get("_name")).getBucketByKey("value2").getDocCount(), equalTo(1l));
    }

    public void testTVApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string,term_vector=with_positions_offsets_payloads", "field2", "type=string,term_vector=with_positions_offsets_payloads")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        Boolean realtime = randomFrom(true, false, null);
        TermVectorsResponse response = client().prepareTermVectors("test", "type1", "1")
                .setRealtime(realtime)
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(1));
        assertThat(response.getFields().terms("field1").size(), equalTo(1l));

        response = client().prepareTermVectors("test", "type1", "1")
                .setRealtime(realtime)
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(1));
        assertThat(response.getFields().terms("field2").size(), equalTo(1l));

        response = client().prepareTermVectors("test", "type1", "1")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD))
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(2));
        assertThat(response.getFields().terms("field1").size(), equalTo(1l));
        assertThat(response.getFields().terms("field2").size(), equalTo(1l));

        response = client().prepareTermVectors("test", "type1", "1")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD))
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getFields().size(), equalTo(0));
    }

    public void testMTVApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string,term_vector=with_positions_offsets_payloads", "field2", "type=string,term_vector=with_positions_offsets_payloads")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        Boolean realtime = randomFrom(true, false, null);
        MultiTermVectorsResponse response = client().prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "type1", "1").realtime(realtime))
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(1));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field1").size(), equalTo(1l));

        response = client().prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "type1", "1").realtime(realtime))
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(1));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field2").size(), equalTo(1l));

        response = client().prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "type1", "1").realtime(realtime))
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(2));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field1").size(), equalTo(1l));
        assertThat(response.getResponses()[0].getResponse().getFields().terms("field2").size(), equalTo(1l));

        response = client().prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "type1", "1").realtime(realtime))
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getFields().size(), equalTo(0));
    }

    public void testPercolateApi() {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping(".percolator", "field1", "type=string", "field2", "type=string")
        );
        client().prepareIndex("test", ".percolator", "1")
                .setSource("{\"query\" : { \"match_all\" : {} }, \"field1\" : \"value1\"}")
                .setRefresh(true)
                .get();

        // Percolator without a query just evaluates all percolator queries that are loaded, so we have a match:
        PercolateResponse response = client().preparePercolate()
                .setDocumentType("type")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                .get();
        assertThat(response.getCount(), equalTo(1l));
        assertThat(response.getMatches()[0].getId().string(), equalTo("1"));

        // Percolator with a query on a field that the current user can't see. Percolator will not have queries to evaluate, so there is no match:
        response = client().preparePercolate()
                .setDocumentType("type")
                .setPercolateQuery(termQuery("field1", "value1"))
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                .get();
        assertThat(response.getCount(), equalTo(0l));

        assertAcked(client().admin().indices().prepareClose("test"));
        assertAcked(client().admin().indices().prepareOpen("test"));
        ensureGreen("test");

        // Ensure that the query loading that happens at startup has permissions to load the percolator queries:
        response = client().preparePercolate()
                .setDocumentType("type")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                .get();
        assertThat(response.getCount(), equalTo(1l));
        assertThat(response.getMatches()[0].getId().string(), equalTo("1"));
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

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(hasChildQuery("child", termQuery("field1", "yellow")))
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                .get();
        assertHitCount(searchResponse, 1l);
        assertThat(searchResponse.getHits().totalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(hasChildQuery("child", termQuery("field1", "yellow")))
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
                .get();
        assertHitCount(searchResponse, 0l);
    }

    public void testUpdateApiIsBlocked() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type", "field1", "type=string", "field2", "type=string")
        );
        client().prepareIndex("test", "type", "1")
                .setSource("field1", "value1", "field2", "value1")
                .setRefresh(true)
                .get();

        // With field level security enabled the update is not allowed:
        try {
            client().prepareUpdate("test", "type", "1").setDoc("field2", "value2")
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
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
        try {
            client().prepareBulk()
                    .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                    .add(new UpdateRequest("test", "type", "1").doc("field2", "value3"))
                    .get();
            fail("failed, because bulk request with updates shouldn't be allowed if field level security is enabled");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(e.getMessage(), equalTo("Can't execute an bulk request with update requests embedded if field or document level security is enabled"));
        }
        assertThat(client().prepareGet("test", "type", "1").get().getSource().get("field2").toString(), equalTo("value2"));

        client().prepareBulk()
                .add(new UpdateRequest("test", "type", "1").doc("field2", "value3"))
                .get();
        assertThat(client().prepareGet("test", "type", "1").get().getSource().get("field2").toString(), equalTo("value3"));
    }

    public void testQuery_withRoleWithFieldWildcards() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=string", "field2", "type=string")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1", "field2", "value2")
                .setRefresh(true)
                .get();

        // user6 has access to all fields, so the query should match with the document:
        SearchResponse response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD))
                .setQuery(matchQuery("field1", "value1"))
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).sourceAsMap().size(), equalTo(2));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field2").toString(), equalTo("value2"));

        response = client().prepareSearch("test")
                .putHeader(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD))
                .setQuery(matchQuery("field2", "value2"))
                .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).sourceAsMap().size(), equalTo(2));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getHits().getAt(0).sourceAsMap().get("field2").toString(), equalTo("value2"));
    }

}
