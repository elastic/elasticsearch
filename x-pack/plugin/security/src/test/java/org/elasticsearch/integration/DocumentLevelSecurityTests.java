/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortMode;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.security.LocalStateSecurity;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.join.query.JoinQueryBuilders.hasChildQuery;
import static org.elasticsearch.join.query.JoinQueryBuilders.hasParentQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@LuceneTestCase.SuppressCodecs("*") // suppress test codecs otherwise test using completion suggester fails
public class DocumentLevelSecurityTests extends SecurityIntegTestCase {

    protected static final SecureString USERS_PASSWD = new SecureString("change_me".toCharArray());

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateSecurity.class, CommonAnalysisPlugin.class, ParentJoinPlugin.class, InternalSettingsPlugin.class);
    }

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(getFastStoredHashAlgoForTests().hash(USERS_PASSWD));
        return super.configUsers() +
            "user1:" + usersPasswdHashed + "\n" +
            "user2:" + usersPasswdHashed + "\n" +
            "user3:" + usersPasswdHashed + "\n" +
            "user4:" + usersPasswdHashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() +
                "role1:user1,user2,user3\n" +
                "role2:user1,user3\n" +
                "role3:user2,user3\n" +
                "role4:user4\n";
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
                "      query: '{\"term\" : {\"field2\" : \"value2\"}}'\n" + // <-- query defined as json in a string
                "role4:\n" +
                "  cluster: [ all ]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ ALL ]\n" +
                // query that can match nested documents
                "      query: '{\"bool\": { \"must_not\": { \"term\" : {\"field1\" : \"value2\"}}}}'";
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(XPackSettings.DLS_FLS_ENABLED.getKey(), true)
                .put(XPackSettings.AUDIT_ENABLED.getKey(), false) // Just to make logs less noisy
                .build();
    }

    public void testSimpleQuery() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        client().prepareIndex("test").setId("1").setSource("field1", "value1")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test").setId("2").setSource("field2", "value2")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test").setId("3").setSource("field3", "value3")
                .setRefreshPolicy(IMMEDIATE)
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
                .minimumShouldMatch(1);
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(randomBoolean() ? combined : QueryBuilders.matchAllQuery())
                .get();
        assertHitCount(response, 2);
        assertSearchHits(response, "1", "2");
    }

    public void testGetApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text")
        );

        client().prepareIndex("test").setId("1").setSource("field1", "value1").get();
        client().prepareIndex("test").setId("2").setSource("field2", "value2").get();
        client().prepareIndex("test").setId("3").setSource("field3", "value3").get();

        // test documents users can see
        boolean realtime = randomBoolean();
        GetResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareGet("test", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getId(), equalTo("1"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareGet("test", "2")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getId(), equalTo("2"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareGet("test","1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getId(), equalTo("1"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareGet("test", "2")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getId(), equalTo("2"));

        // test documents user cannot see
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareGet("test", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(false));
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareGet("test", "2")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(false));
        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareGet("test", "3")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.isExists(), is(false));
    }

    public void testMGetApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text")
        );

        client().prepareIndex("test").setId("1").setSource("field1", "value1").get();
        client().prepareIndex("test").setId("2").setSource("field2", "value2").get();
        client().prepareIndex("test").setId("3").setSource("field3", "value3").get();

        boolean realtime = randomBoolean();
        MultiGetResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getId(), equalTo("1"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "2")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getId(), equalTo("2"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "1")
                .add("test", "2")
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
                .add("test", "1")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(false));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "2")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(false));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareMultiGet()
                .add("test", "3")
                .setRealtime(realtime)
                .setRefresh(true)
                .get();
        assertThat(response.getResponses()[0].isFailed(), is(false));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(false));
    }

    public void testMSearch() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test1")
                .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text", "id", "type=integer")
        );
        assertAcked(client().admin().indices().prepareCreate("test2")
                .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text", "id", "type=integer")
        );

        client().prepareIndex("test1").setId("1").setSource("field1", "value1", "id", 1).get();
        client().prepareIndex("test1").setId("2").setSource("field2", "value2", "id", 2).get();
        client().prepareIndex("test1").setId("3").setSource("field3", "value3", "id", 3).get();
        client().prepareIndex("test2").setId("1").setSource("field1", "value1", "id", 1).get();
        client().prepareIndex("test2").setId("2").setSource("field2", "value2", "id", 2).get();
        client().prepareIndex("test2").setId("3").setSource("field3", "value3", "id", 3).get();
        client().admin().indices().prepareRefresh("test1", "test2").get();

        MultiSearchResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareMultiSearch()
                .add(client().prepareSearch("test1").setQuery(QueryBuilders.matchAllQuery()))
                .add(client().prepareSearch("test2").setQuery(QueryBuilders.matchAllQuery()))
                .get();
        assertFalse(response.getResponses()[0].isFailure());
        assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits().value, is(1L));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSourceAsMap().size(), is(2));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSourceAsMap().get("field1"), is("value1"));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSourceAsMap().get("id"), is(1));

        assertFalse(response.getResponses()[1].isFailure());
        assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits().value, is(1L));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSourceAsMap().size(), is(2));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSourceAsMap().get("field1"), is("value1"));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSourceAsMap().get("id"), is(1));

        response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareMultiSearch()
                .add(client().prepareSearch("test1").setQuery(QueryBuilders.matchAllQuery()))
                .add(client().prepareSearch("test2").setQuery(QueryBuilders.matchAllQuery()))
                .get();
        assertFalse(response.getResponses()[0].isFailure());
        assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits().value, is(1L));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSourceAsMap().size(), is(2));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSourceAsMap().get("field2"), is("value2"));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSourceAsMap().get("id"), is(2));

        assertFalse(response.getResponses()[1].isFailure());
        assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits().value, is(1L));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSourceAsMap().size(), is(2));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSourceAsMap().get("field2"), is("value2"));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSourceAsMap().get("id"), is(2));

        response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareMultiSearch()
                .add(client().prepareSearch("test1").addSort(SortBuilders.fieldSort("id").sortMode(SortMode.MIN))
                        .setQuery(QueryBuilders.matchAllQuery()))
                .add(client().prepareSearch("test2").addSort(SortBuilders.fieldSort("id").sortMode(SortMode.MIN))
                        .setQuery(QueryBuilders.matchAllQuery()))
                .get();
        assertFalse(response.getResponses()[0].isFailure());
        assertThat(response.getResponses()[0].getResponse().getHits().getTotalHits().value, is(2L));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSourceAsMap().size(), is(2));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSourceAsMap().get("field1"), is("value1"));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(0).getSourceAsMap().get("id"), is(1));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(1).getSourceAsMap().size(), is(2));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(1).getSourceAsMap().get("field2"), is("value2"));
        assertThat(response.getResponses()[0].getResponse().getHits().getAt(1).getSourceAsMap().get("id"), is(2));

        assertFalse(response.getResponses()[1].isFailure());
        assertThat(response.getResponses()[1].getResponse().getHits().getTotalHits().value, is(2L));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSourceAsMap().size(), is(2));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSourceAsMap().get("field1"), is("value1"));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(0).getSourceAsMap().get("id"), is(1));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(1).getSourceAsMap().size(), is(2));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(1).getSourceAsMap().get("field2"), is("value2"));
        assertThat(response.getResponses()[1].getResponse().getHits().getAt(1).getSourceAsMap().get("id"), is(2));
    }

    public void testTVApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .setMapping("field1", "type=text,term_vector=with_positions_offsets_payloads",
                                "field2", "type=text,term_vector=with_positions_offsets_payloads",
                                "field3", "type=text,term_vector=with_positions_offsets_payloads")
        );
        client().prepareIndex("test").setId("1").setSource("field1", "value1")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test").setId("2").setSource("field2", "value2")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test").setId("3").setSource("field3", "value3")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        boolean realtime = randomBoolean();
        TermVectorsResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareTermVectors("test", "1")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getId(), is("1"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareTermVectors("test", "2")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getId(), is("2"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareTermVectors("test", "1")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getId(), is("1"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareTermVectors("test", "2")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(true));
        assertThat(response.getId(), is("2"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareTermVectors("test", "1")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(false));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareTermVectors("test", "2")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(false));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareTermVectors("test", "3")
                .setRealtime(realtime)
                .get();
        assertThat(response.isExists(), is(false));
    }

    public void testMTVApi() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .setMapping("field1", "type=text,term_vector=with_positions_offsets_payloads",
                                "field2", "type=text,term_vector=with_positions_offsets_payloads",
                                "field3", "type=text,term_vector=with_positions_offsets_payloads")
        );
        client().prepareIndex("test").setId("1").setSource("field1", "value1")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test").setId("2").setSource("field2", "value2")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test").setId("3").setSource("field3", "value3")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        boolean realtime = randomBoolean();
        MultiTermVectorsResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "1").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getId(), is("1"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "2").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getId(), is("2"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "1").realtime(realtime)).add(new TermVectorsRequest("test", "2").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(2));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[0].getResponse().getId(), is("1"));
        assertThat(response.getResponses()[1].getResponse().isExists(), is(true));
        assertThat(response.getResponses()[1].getResponse().getId(), is("2"));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "1").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(false));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "2").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(false));

        response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareMultiTermVectors()
                .add(new TermVectorsRequest("test", "3").realtime(realtime))
                .get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), is(false));
    }

    public void testGlobalAggregation() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                        .setMapping("field1", "type=text", "field2", "type=text,fielddata=true", "field3", "type=text")
        );
        client().prepareIndex("test").setId("1").setSource("field1", "value1")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test").setId("2").setSource("field2", "value2")
                .setRefreshPolicy(IMMEDIATE)
                .get();
        client().prepareIndex("test").setId("3").setSource("field3", "value3")
                .setRefreshPolicy(IMMEDIATE)
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

    public void testParentChild() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("id")
                        .field("type", "keyword")
                    .endObject()
                    .startObject("join_field")
                        .field("type", "join")
                        .startObject("relations")
                            .field("parent", "child")
                        .endObject()
                    .endObject()
                    .startObject("field1")
                      .field("type", "text")
                    .endObject()
                    .startObject("field2")
                      .field("type", "text")
                    .endObject()
                    .startObject("field3")
                      .field("type", "text")
                    .endObject()
                .endObject()
                .endObject();
        assertAcked(prepareCreate("test")
                .setMapping(mapping));
        ensureGreen();

        // index simple data
        client().prepareIndex("test").setId("p1").setSource("join_field", "parent", "field1", "value1").get();

        Map<String, Object> source = new HashMap<>();
        source.put("field2", "value2");
        source.put("id", "c1");
        Map<String, Object> joinField = new HashMap<>();
        joinField.put("name", "child");
        joinField.put("parent", "p1");
        source.put("join_field", joinField);
        client().prepareIndex("test").setId("c1").setSource(source).setRouting("p1").get();
        source.put("id", "c2");
        client().prepareIndex("test").setId("c2").setSource(source).setRouting("p1").get();
        source = new HashMap<>();
        source.put("field3", "value3");
        source.put("join_field", joinField);
        source.put("id", "c3");
        client().prepareIndex("test").setId("c3").setSource(source).setRouting("p1").get();
        refresh();
        verifyParentChild();
    }

    private void verifyParentChild() {
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(hasChildQuery("child", matchAllQuery(), ScoreMode.None))
                .get();
        assertHitCount(searchResponse, 1L);
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("p1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(hasParentQuery("parent", matchAllQuery(), false))
                .addSort("id", SortOrder.ASC)
                .get();
        assertHitCount(searchResponse, 3L);
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("c1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("c2"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("c3"));

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
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("p1"));

        searchResponse = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasParentQuery("parent", matchAllQuery(), false))
                .get();
        assertHitCount(searchResponse, 2L);
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("c1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("c2"));
    }

    public void testScroll() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true))
                .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        final int numVisible = scaledRandomIntBetween(2, 10);
        final int numInVisible = scaledRandomIntBetween(2, 10);
        int id = 1;
        for (int i = 0; i < numVisible; i++) {
            client().prepareIndex("test").setId(String.valueOf(id++)).setSource("field1", "value1").get();
        }

        for (int i = 0; i < numInVisible; i++) {
            client().prepareIndex("test").setId(String.valueOf(id++)).setSource("field2", "value2").get();
            client().prepareIndex("test").setId(String.valueOf(id++)).setSource("field3", "value3").get();
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
                assertThat(response.getHits().getTotalHits().value, is((long) numVisible));
                assertThat(response.getHits().getAt(0).getSourceAsMap().size(), is(1));
                assertThat(response.getHits().getAt(0).getSourceAsMap().get("field1"), is("value1"));

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
                .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        client().prepareIndex("test").setId("1").setSource("field1", "value1")
                .get();
        client().prepareIndex("test").setId("2").setSource("field2", "value2")
                .get();
        client().prepareIndex("test").setId("3").setSource("field3", "value3")
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
                .setMapping("field1", "type=text", "field2", "type=text")
        );
        client().prepareIndex("test").setId("1").setSource("field1", "value1")
                .setRefreshPolicy(IMMEDIATE)
                .get();

        // With document level security enabled the update is not allowed:
        try {
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .prepareUpdate("test", "1").setDoc(Requests.INDEX_CONTENT_TYPE, "field1", "value2")
                    .get();
            fail("failed, because update request shouldn't be allowed if document level security is enabled");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(e.getMessage(), equalTo("Can't execute an update request if field or document level security is enabled"));
        }
        assertThat(client().prepareGet("test", "1").get().getSource().get("field1").toString(), equalTo("value1"));

        // With no document level security enabled the update is allowed:
        client().prepareUpdate("test", "1").setDoc(Requests.INDEX_CONTENT_TYPE, "field1", "value2")
                .get();
        assertThat(client().prepareGet("test", "1").get().getSource().get("field1").toString(), equalTo("value2"));

        // With document level security enabled the update in bulk is not allowed:
        BulkResponse bulkResponse = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue
                ("user1", USERS_PASSWD)))
                .prepareBulk()
                .add(new UpdateRequest("test", "1").doc(Requests.INDEX_CONTENT_TYPE, "field1", "value3"))
                .get();
        assertEquals(1, bulkResponse.getItems().length);
        BulkItemResponse bulkItem = bulkResponse.getItems()[0];
        assertTrue(bulkItem.isFailed());
        assertThat(bulkItem.getFailure().getCause(), instanceOf(ElasticsearchSecurityException.class));
        ElasticsearchSecurityException securityException = (ElasticsearchSecurityException) bulkItem.getFailure().getCause();
        assertThat(securityException.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(securityException.getMessage(),
                equalTo("Can't execute a bulk item request with update requests embedded if field or document level security is enabled"));

        assertThat(client().prepareGet("test", "1").get().getSource().get("field1").toString(), equalTo("value2"));

        client().prepareBulk()
                .add(new UpdateRequest("test", "1").doc(Requests.INDEX_CONTENT_TYPE, "field1", "value3"))
                .get();
        assertThat(client().prepareGet("test", "1").get().getSource().get("field1").toString(), equalTo("value3"));
    }

    public void testNestedInnerHits() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setMapping("field1", "type=text", "nested_field", "type=nested")
        );
        client().prepareIndex("test").setId("1")
                .setSource(jsonBuilder().startObject()
                            .field("field1", "value1")
                            .startArray("nested_field")
                                .startObject()
                                    .field("field2", "value2")
                                .endObject()
                                .startObject()
                                    .array("field2", "value2", "value3")
                                .endObject()
                            .endArray()
                        .endObject())
                .get();
        client().prepareIndex("test").setId("2")
                .setSource(jsonBuilder().startObject()
                            .field("field1", "value2")
                            .startArray("nested_field")
                                .startObject()
                                    .field("field2", "value2")
                                .endObject()
                            .endArray()
                        .endObject())
                .get();
        refresh("test");

        SearchResponse response = client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(QueryBuilders.nestedQuery("nested_field", QueryBuilders.termQuery("nested_field.field2", "value2"),
                        ScoreMode.None).innerHit(new InnerHitBuilder()))
                .get();
        assertHitCount(response, 1);
        assertSearchHits(response, "1");
        assertThat(response.getHits().getAt(0).getInnerHits().get("nested_field").getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("nested_field").getAt(0).getNestedIdentity().getOffset(), equalTo(0));
        assertThat(response.getHits().getAt(0).getInnerHits().get("nested_field").getAt(0).getSourceAsString(),
                equalTo("{\"field2\":\"value2\"}"));
        assertThat(response.getHits().getAt(0).getInnerHits().get("nested_field").getAt(1).getNestedIdentity().getOffset(), equalTo(1));
        assertThat(response.getHits().getAt(0).getInnerHits().get("nested_field").getAt(1).getSourceAsString(),
            equalTo("{\"field2\":[\"value2\",\"value3\"]}"));
    }

    public void testSuggesters() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                )
                .setMapping("field1", "type=text", "suggest_field1", "type=text", "suggest_field2", "type=completion")
        );

        client().prepareIndex("test").setId("1")
                .setSource(jsonBuilder().startObject()
                        .field("field1", "value1")
                        .field("suggest_field1", "value")
                        .startObject("suggest_field2")
                            .field("input", "value")
                        .endObject()
                        .endObject()).get();
        // A document that is always included by role query of both roles:
        client().prepareIndex("test").setId("2")
                .setSource(jsonBuilder().startObject()
                        .field("field1", "value1")
                        .field("field2", "value2")
                        .endObject()).get();
        refresh("test");

        // Term suggester:
        SearchResponse response = client()
                .prepareSearch("test")
                .suggest(new SuggestBuilder()
                        .setGlobalText("valeu")
                        .addSuggestion("_name1", new TermSuggestionBuilder("suggest_field1"))
                ).get();
        assertNoFailures(response);

        TermSuggestion termSuggestion = response.getSuggest().getSuggestion("_name1");
        assertThat(termSuggestion, notNullValue());
        assertThat(termSuggestion.getEntries().size(), equalTo(1));
        assertThat(termSuggestion.getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(termSuggestion.getEntries().get(0).getOptions().get(0).getText().string(), equalTo("value"));

        Exception e = expectThrows(ElasticsearchSecurityException.class, () -> client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .suggest(new SuggestBuilder()
                        .setGlobalText("valeu")
                        .addSuggestion("_name1", new TermSuggestionBuilder("suggest_field1"))
                ).get());
        assertThat(e.getMessage(), equalTo("Suggest isn't supported if document level security is enabled"));

        // Phrase suggester:
        response = client()
                .prepareSearch("test")
                .suggest(new SuggestBuilder()
                        .setGlobalText("valeu")
                        .addSuggestion("_name1", new PhraseSuggestionBuilder("suggest_field1"))
                ).get();
        assertNoFailures(response);

        PhraseSuggestion phraseSuggestion = response.getSuggest().getSuggestion("_name1");
        assertThat(phraseSuggestion, notNullValue());
        assertThat(phraseSuggestion.getEntries().size(), equalTo(1));
        assertThat(phraseSuggestion.getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(phraseSuggestion.getEntries().get(0).getOptions().get(0).getText().string(), equalTo("value"));

        e = expectThrows(ElasticsearchSecurityException.class, () -> client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .suggest(new SuggestBuilder()
                        .setGlobalText("valeu")
                        .addSuggestion("_name1", new PhraseSuggestionBuilder("suggest_field1"))
                ).get());
        assertThat(e.getMessage(), equalTo("Suggest isn't supported if document level security is enabled"));

        // Completion suggester:
        response = client()
                .prepareSearch("test")
                .suggest(new SuggestBuilder()
                        .setGlobalText("valu")
                        .addSuggestion("_name1", new CompletionSuggestionBuilder("suggest_field2"))
                ).get();
        assertNoFailures(response);

        CompletionSuggestion completionSuggestion = response.getSuggest().getSuggestion("_name1");
        assertThat(completionSuggestion, notNullValue());
        assertThat(completionSuggestion.getEntries().size(), equalTo(1));
        assertThat(completionSuggestion.getEntries().get(0).getOptions().size(), equalTo(1));
        assertThat(completionSuggestion.getEntries().get(0).getOptions().get(0).getText().string(), equalTo("value"));

        e = expectThrows(ElasticsearchSecurityException.class, () -> client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .suggest(new SuggestBuilder()
                        .setGlobalText("valeu")
                        .addSuggestion("_name1", new CompletionSuggestionBuilder("suggest_field2"))
                ).get());
        assertThat(e.getMessage(), equalTo("Suggest isn't supported if document level security is enabled"));
    }

    public void testProfile() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                )
                .setMapping("field1", "type=text", "other_field", "type=text")
        );

        client().prepareIndex("test").setId("1")
                .setSource(jsonBuilder().startObject()
                        .field("field1", "value1")
                        .field("other_field", "value")
                        .endObject()).get();
        // A document that is always included by role query of both roles:
        client().prepareIndex("test").setId("2")
                .setSource(jsonBuilder().startObject()
                        .field("field1", "value1")
                        .field("field2", "value2")
                        .endObject()).get();
        refresh("test");

        SearchResponse response = client()
                .prepareSearch("test")
                .setProfile(true)
                .setQuery(new FuzzyQueryBuilder("other_field", "valeu"))
                .get();
        assertNoFailures(response);

        assertThat(response.getProfileResults().size(), equalTo(1));
        ProfileShardResult shardResult = response.getProfileResults().get(response.getProfileResults().keySet().toArray()[0]);
        assertThat(shardResult.getQueryProfileResults().size(), equalTo(1));
        QueryProfileShardResult queryProfileShardResult = shardResult.getQueryProfileResults().get(0);
        assertThat(queryProfileShardResult.getQueryResults().size(), equalTo(1));
        logger.info("queryProfileShardResult=" + Strings.toString(queryProfileShardResult));
//        ProfileResult profileResult = queryProfileShardResult.getQueryResults().get(0);
//        assertThat(profileResult.getLuceneDescription(), equalTo("(other_field:value)^0.8"));

        Exception e = expectThrows(ElasticsearchSecurityException.class, () -> client()
                .filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setProfile(true)
                .setQuery(new FuzzyQueryBuilder("other_field", "valeu"))
                .get());
        assertThat(e.getMessage(), equalTo("A search request cannot be profiled if document level security is enabled"));
    }

}
