/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchSecurityException;
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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.children.Children;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.shield.Security;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.xpack.XPackPlugin;

import java.util.Collections;

import static org.elasticsearch.index.query.QueryBuilders.hasChildQuery;
import static org.elasticsearch.index.query.QueryBuilders.hasParentQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 */
public class DocumentLevelSecurityTests extends ShieldIntegTestCase {

    protected static final SecuredString USERS_PASSWD = new SecuredString("change_me".toCharArray());
    protected static final String USERS_PASSWD_HASHED = new String(Hasher.BCRYPT.hash(USERS_PASSWD));

    @Override
    protected String configUsers() {
        return super.configUsers() +
                "user1:" + USERS_PASSWD_HASHED + "\n" +
                "user2:" + USERS_PASSWD_HASHED + "\n" +
                "user3:" + USERS_PASSWD_HASHED + "\n" ;
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() +
                "role1:user1,user2,user3\n" +
                "role2:user1,user3\n" +
                "role3:user2,user3\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles() +
                "\nrole1:\n" +
                "  cluster: [ none ]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ none ]\n" +
                "\nrole2:\n" +
                "  cluster:\n" +
                "    - all\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges:\n" +
                "        - all\n" +
                "      query: \n" +
                "        term: \n" +
                "          field1: value1\n" +
                "role3:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ ALL ]\n" +
                "      query: '{\"term\" : {\"field2\" : \"value2\"}}'"; // <-- query defined as json in a string
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(XPackPlugin.featureEnabledSetting(Security.DLS_FLS_FEATURE), true)
                .build();
    }

    public void testSimpleQuery() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1")
                .setRefresh(true)
                .get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value2")
                .setRefresh(true)
                .get();
        client().prepareIndex("test", "type1", "3").setSource("field3", "value3")
                .setRefresh(true)
                .get();

        SearchResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(randomBoolean() ? QueryBuilders.termQuery("field1", "value1") : QueryBuilders.matchAllQuery())
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "1");

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(randomBoolean() ? QueryBuilders.termQuery("field2", "value2") : QueryBuilders.matchAllQuery())
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "2");

        QueryBuilder combined = QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("field2", "value2"))
                .should(QueryBuilders.termQuery("field1", "value1"))
                .minimumNumberShouldMatch(1);
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(randomBoolean() ? combined : QueryBuilders.matchAllQuery())
                .get();
        assertHitCount(response, 2);
        assertSearchHits(response, "1", "2");
    }

    public void testGetApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text", "field2", "type=text", "field3", "type=text")
        );

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value2").get();
        client().prepareIndex("test", "type1", "3").setSource("field3", "value3").get();

        // test documents users can see
        boolean realtime = randomBoolean();
        GetResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareGet("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getId(), equalTo("1"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareGet("test", "type1", "2")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getId(), equalTo("2"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareGet("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getId(), equalTo("1"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareGet("test", "type1", "2")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getId(), equalTo("2"));

        // test documents user cannot see
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareGet("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(false));
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareGet("test", "type1", "2")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(false));
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareGet("test", "type1", "3")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(false));
    }

    public void testMGetApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text", "field2", "type=text", "field3", "type=text")
        );

        client().prepareIndex("test", "type1", "1").setSource("field1", "value1").get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value2").get();
        client().prepareIndex("test", "type1", "3").setSource("field3", "value3").get();

        boolean realtime = randomBoolean();
        MultiGetResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getId(), equalTo("1"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "type1", "2")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getId(), equalTo("2"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "type1", "1")
                .add("test", "type1", "2")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getId(), equalTo("1"));
        assertThat(response.getResponses()[1].isFailed(), is(false));
        assertThat(response.getResponses()[1].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[1].getResponse().getId(), equalTo("2"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "type1", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(false));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "type1", "2")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(false));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "type1", "3")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(false));
    }

    public void testTVApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text,term_vector=with_positions_offsets_payloads",
                                "field2", "type=text,term_vector=with_positions_offsets_payloads",
                                "field3", "type=text,term_vector=with_positions_offsets_payloads")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1")
                .setRefresh(true)
                .get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value2")
                .setRefresh(true)
                .get();
        client().prepareIndex("test", "type1", "3").setSource("field3", "value3")
                .setRefresh(true)
                .get();

        boolean realtime = randomBoolean();
        TermVectorsResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareTermVectors("test", "type1", "1")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getId(), is("1"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareTermVectors("test", "type1", "2")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getId(), is("2"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareTermVectors("test", "type1", "1")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getId(), is("1"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareTermVectors("test", "type1", "2")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getId(), is("2"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareTermVectors("test", "type1", "1")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(false));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareTermVectors("test", "type1", "2")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(false));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareTermVectors("test", "type1", "3")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(false));
    }

    public void testMTVApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text,term_vector=with_positions_offsets_payloads",
                                "field2", "type=text,term_vector=with_positions_offsets_payloads",
                                "field3", "type=text,term_vector=with_positions_offsets_payloads")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1")
                .setRefresh(true)
                .get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value2")
                .setRefresh(true)
                .get();
        client().prepareIndex("test", "type1", "3").setSource("field3", "value3")
                .setRefresh(true)
                .get();

        boolean realtime = randomBoolean();
        MultiTermVectorsResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "type1", "1").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getId(), is("1"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "type1", "2").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getId(), is("2"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "type1", "1").realtime(realtime))
                .add(new TermVectorsRequest("test", "type1", "2").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(2));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getId(), is("1"));
        assertThat(response.getResponses()[1].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[1].getResponse().getId(), is("2"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "type1", "1").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(false));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "type1", "2").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(false));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "type1", "3").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(false));
    }

    public void testGlobalAggregation() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text", "field2", "type=text,fielddata=true", "field3", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1")
                .setRefresh(true)
                .get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value2")
                .setRefresh(true)
                .get();
        client().prepareIndex("test", "type1", "3").setSource("field3", "value3")
                .setRefresh(true)
                .get();

        SearchResponse response = client().prepareSearch("test")
                .addAggregation(AggregationBuilders.global("global").subAggregation(AggregationBuilders.terms("field2").field("field2")))
                .get();
        assertHitCount(response, 3);
        assertSearchHits(response, "1", "2", "3");

        Global globalAgg = response.getAggregations().get("global");
        assertThat(globalAgg.getDocCount(), equalTo(3L));
        Terms termsAgg = globalAgg.getAggregations().get("field2");
        assertThat(termsAgg.getBuckets().get(0).getKeyAsString(), equalTo("value2"));
        assertThat(termsAgg.getBuckets().get(0).getDocCount(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .addAggregation(AggregationBuilders.global("global").subAggregation(AggregationBuilders.terms("field2").field("field2")))
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "1");

        globalAgg = response.getAggregations().get("global");
        assertThat(globalAgg.getDocCount(), equalTo(1L));
        termsAgg = globalAgg.getAggregations().get("field2");
        assertThat(termsAgg.getBuckets().size(), equalTo(0));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .addAggregation(AggregationBuilders.global("global").subAggregation(AggregationBuilders.terms("field2").field("field2")))
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "2");

        globalAgg = response.getAggregations().get("global");
        assertThat(globalAgg.getDocCount(), equalTo(1L));
        termsAgg = globalAgg.getAggregations().get("field2");
        assertThat(termsAgg.getBuckets().size(), equalTo(1));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .addAggregation(AggregationBuilders.global("global").subAggregation(AggregationBuilders.terms("field2").field("field2")))
                .get();
        assertHitCount(response, 2);
        assertSearchHits(response, "1", "2");

        globalAgg = response.getAggregations().get("global");
        assertThat(globalAgg.getDocCount(), equalTo(2L));
        termsAgg = globalAgg.getAggregations().get("field2");
        assertThat(termsAgg.getBuckets().size(), equalTo(1));
    }

    public void testChildrenAggregation() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .addMapping("type1", "field1", "type=text", "field2", "type=text")
                        .addMapping("type2", "_parent", "type=type1", "field3", "type=text,fielddata=true")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1")
                .setRefresh(true)
                .get();
        client().prepareIndex("test", "type2", "2").setSource("field3", "value3")
                .setParent("1")
                .setRefresh(true)
                .get();

        SearchResponse response = client().prepareSearch("test")
                .setTypes("type1")
                .addAggregation(AggregationBuilders.children("children", "type2")
                        .subAggregation(AggregationBuilders.terms("field3").field("field3")))
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "1");

        Children children = response.getAggregations().get("children");
        assertThat(children.getDocCount(), equalTo(1L));
        Terms termsAgg = children.getAggregations().get("field3");
        assertThat(termsAgg.getBuckets().get(0).getKeyAsString(), equalTo("value3"));
        assertThat(termsAgg.getBuckets().get(0).getDocCount(), equalTo(1L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setTypes("type1")
                .addAggregation(AggregationBuilders.children("children", "type2")
                        .subAggregation(AggregationBuilders.terms("field3").field("field3")))
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "1");

        children = response.getAggregations().get("children");
        assertThat(children.getDocCount(), equalTo(0L));
        termsAgg = children.getAggregations().get("field3");
        assertThat(termsAgg.getBuckets().size(), equalTo(0));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setTypes("type1")
                .addAggregation(AggregationBuilders.children("children", "type2")
                        .subAggregation(AggregationBuilders.terms("field3").field("field3")))
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "1");

        children = response.getAggregations().get("children");
        assertThat(children.getDocCount(), equalTo(0L));
        termsAgg = children.getAggregations().get("field3");
        assertThat(termsAgg.getBuckets().size(), equalTo(0));
    }

    public void testParentChild() {
        assertAcked(prepareCreate("test")
                .addMapping("parent")
                .addMapping("child", "_parent", "type=parent", "field1", "type=text", "field2", "type=text", "field3", "type=text"));
        ensureGreen();

        // index simple data
        client().prepareIndex("test", "parent", "p1").setSource("field1", "value1").get();
        client().prepareIndex("test", "child", "c1").setSource("field2", "value2").setParent("p1").get();
        client().prepareIndex("test", "child", "c2").setSource("field2", "value2").setParent("p1").get();
        client().prepareIndex("test", "child", "c3").setSource("field3", "value3").setParent("p1").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(hasChildQuery("child", matchAllQuery(), ScoreMode.None))
                .get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(hasParentQuery("parent", matchAllQuery(), false))
                .addSort("_uid", SortOrder.ASC)
                .get();
        assertHitCount(searchResponse, 3L);
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c1"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("c2"));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("c3"));

        // Both user1 and user2 can't see field1 and field2, no parent/child query should yield results:
        searchResponse = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasChildQuery("child", matchAllQuery(), ScoreMode.None))
                .get();
        assertHitCount(searchResponse, 0L);

        searchResponse = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasChildQuery("child", matchAllQuery(), ScoreMode.None))
                .get();
        assertHitCount(searchResponse, 0L);

        searchResponse = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasParentQuery("parent", matchAllQuery(), false))
                .get();
        assertHitCount(searchResponse, 0L);

        searchResponse = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasParentQuery("parent", matchAllQuery(), false))
                .get();
        assertHitCount(searchResponse, 0L);

        // user 3 can see them but not c3
        searchResponse = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasChildQuery("child", matchAllQuery(), ScoreMode.None))
                .get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("p1"));

        searchResponse = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasParentQuery("parent", matchAllQuery(), false))
                .get();
        assertHitCount(searchResponse, 2L);
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("c1"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("c2"));
    }

    public void testPercolateApi() {
        assertAcked(client().admin().indices().prepareCreate("test")
            .addMapping("query", "query", "type=percolator", "field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        client().prepareIndex("test", "query", "1")
                .setSource("{\"query\" : { \"match_all\" : {} }, \"field1\" : \"value1\"}")
                .setRefresh(true)
                .get();

        // Percolator without a query just evaluates all percolator queries that are loaded, so we have a match:
        PercolateResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .preparePercolate()
                .setDocumentType("query")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .get();
        assertThat(response.getCount(), equalTo(1L));
        assertThat(response.getMatches()[0].getId().string(), equalTo("1"));

        response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .preparePercolate()
                .setDocumentType("query")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .get();
        assertThat(response.getCount(), equalTo(0L));

        response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .preparePercolate()
                .setDocumentType("query")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .get();
        assertThat(response.getCount(), equalTo(1L));
        assertThat(response.getMatches()[0].getId().string(), equalTo("1"));

        // Percolator with a query on a document that the current user can see. Percolator will have one query to evaluate, so there is a
        // match:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .preparePercolate()
                .setDocumentType("query")
                .setPercolateQuery(termQuery("field1", "value1"))
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .get();
        assertThat(response.getCount(), equalTo(1L));
        assertThat(response.getMatches()[0].getId().string(), equalTo("1"));

        // Percolator with a query on a document that the current user can't see. Percolator will not have queries to evaluate, so there
        // is no match:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .preparePercolate()
                .setDocumentType("query")
                .setPercolateQuery(termQuery("field1", "value1"))
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .get();
        assertThat(response.getCount(), equalTo(0L));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .preparePercolate()
                .setDocumentType("query")
                .setPercolateQuery(termQuery("field1", "value1"))
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .get();
        assertThat(response.getCount(), equalTo(1L));
        assertThat(response.getMatches()[0].getId().string(), equalTo("1"));

        assertAcked(client().admin().indices().prepareClose("test"));
        assertAcked(client().admin().indices().prepareOpen("test"));
        ensureGreen("test");

        // Ensure that the query loading that happens at startup has permissions to load the percolator queries:
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .preparePercolate()
                .setDocumentType("query")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .get();
        assertThat(response.getCount(), equalTo(1L));
        assertThat(response.getMatches()[0].getId().string(), equalTo("1"));
    }

    public void testScroll() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true))
                .addMapping("type1", "field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        final int numVisible = scaledRandomIntBetween(2, 10);
        final int numInVisible = scaledRandomIntBetween(2, 10);
        int id = 1;
        for (int i = 0; i < numVisible; i++) {
            client().prepareIndex("test", "type1", String.valueOf(id++)).setSource("field1", "value1").get();
        }

        for (int i = 0; i < numInVisible; i++) {
            client().prepareIndex("test", "type1", String.valueOf(id++)).setSource("field2", "value2").get();
            client().prepareIndex("test", "type1", String.valueOf(id++)).setSource("field3", "value3").get();
        }
        refresh();

        SearchResponse response = null;
        try {
            response = client()
                    .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .prepareSearch("test")
                    .setSize(1)
                    .setScroll(TimeValue.timeValueMinutes(1L))
                    .setQuery(termQuery("field1", "value1"))
                    .get();
            do {
                assertNoFailures(response);
                assertThat(response.getHits().getTotalHits(), is((long) numVisible));
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

    public void testRequestCache() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true))
                .addMapping("type1", "field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        client().prepareIndex("test", "type1", "1").setSource("field1", "value1")
                .get();
        client().prepareIndex("test", "type1", "2").setSource("field2", "value2")
                .get();
        client().prepareIndex("test", "type1", "3").setSource("field3", "value3")
                .get();
        refresh();

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
            response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                    .prepareSearch("test")
                    .setSize(0)
                    .setQuery(termQuery("field1", "value1"))
                    .setRequestCache(requestCache)
                    .get();
            assertNoFailures(response);
            assertHitCount(response, 1);
        }
    }

    public void testUpdateApiIsBlocked() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .addMapping("type", "field1", "type=text", "field2", "type=text")
        );
        client().prepareIndex("test", "type", "1").setSource("field1", "value1")
                .setRefresh(true)
                .get();

        // With document level security enabled the update is not allowed:
        try {
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .prepareUpdate("test", "type", "1").setDoc("field1", "value2")
                    .get();
            fail("failed, because update request shouldn't be allowed if document level security is enabled");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(e.getMessage(), equalTo("Can't execute an update request if field or document level security is enabled"));
        }
        assertThat(client().prepareGet("test", "type", "1").get().getSource().get("field1").toString(), equalTo("value1"));

        // With no document level security enabled the update is allowed:
        client().prepareUpdate("test", "type", "1").setDoc("field1", "value2")
                .get();
        assertThat(client().prepareGet("test", "type", "1").get().getSource().get("field1").toString(), equalTo("value2"));

        // With document level security enabled the update in bulk is not allowed:
        try {
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .prepareBulk()
                    .add(new UpdateRequest("test", "type", "1").doc("field1", "value3"))
                    .get();
            fail("failed, because bulk request with updates shouldn't be allowed if field or document level security is enabled");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(e.getMessage(),
                    equalTo("Can't execute an bulk request with update requests embedded if field or document level security is enabled"));
        }
        assertThat(client().prepareGet("test", "type", "1").get().getSource().get("field1").toString(), equalTo("value2"));

        client().prepareBulk()
                .add(new UpdateRequest("test", "type", "1").doc("field1", "value3"))
                .get();
        assertThat(client().prepareGet("test", "type", "1").get().getSource().get("field1").toString(), equalTo("value3"));
    }

}
