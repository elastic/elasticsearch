/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.percolator.PercolateQueryBuilder;
import org.elasticsearch.percolator.PercolatorPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.SearchProfileShardResult;
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
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.elasticsearch.xpack.spatial.index.query.ShapeQueryBuilder;
import org.elasticsearch.xpack.wildcard.Wildcard;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.integration.FieldLevelSecurityTests.openPointInTime;
import static org.elasticsearch.join.query.JoinQueryBuilders.hasChildQuery;
import static org.elasticsearch.join.query.JoinQueryBuilders.hasParentQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@LuceneTestCase.SuppressCodecs("*") // suppress test codecs otherwise test using completion suggester fails
public class DocumentLevelSecurityTests extends SecurityIntegTestCase {

    protected static final SecureString USERS_PASSWD = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            LocalStateSecurity.class,
            CommonAnalysisPlugin.class,
            ParentJoinPlugin.class,
            InternalSettingsPlugin.class,
            SpatialPlugin.class,
            PercolatorPlugin.class,
            MapperExtrasPlugin.class,
            Wildcard.class
        );
    }

    @Override
    protected String configUsers() {
        final String usersPasswdHashed = new String(getFastStoredHashAlgoForTests().hash(USERS_PASSWD));
        return super.configUsers() + Strings.format("""
            user1:%s
            user2:%s
            user3:%s
            user4:%s
            user5:%s
            user6:%s
            """, usersPasswdHashed, usersPasswdHashed, usersPasswdHashed, usersPasswdHashed, usersPasswdHashed, usersPasswdHashed);
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + """
            role1:user1,user2,user3
            role2:user1,user3
            role3:user2,user3
            role4:user4
            role5:user5
            role6:user6
            """;
    }

    @Override
    protected String configRoles() {
        // <-- query defined as json in a string
        // query that can match nested documents
        return super.configRoles() + """

            role1:
              cluster: [ none ]
              indices:
                - names: '*'
                  privileges: [ none ]

            role2:
              cluster:
                - all
              indices:
                - names: '*'
                  privileges:
                    - all
                  query:
                    term:
                      field1: value1
            role3:
              cluster: [ all ]
              indices:
                - names: '*'
                  privileges: [ ALL ]
                  query: '{"term" : {"field2" : "value2"}}'
            role4:
              cluster: [ all ]
              indices:
                - names: '*'
                  privileges: [ ALL ]
                  query: '{"bool": { "must_not": { "term" : {"field1" : "value2"}}}}'
            role5:
              cluster: [ all ]
              indices:
                - names: [ 'test' ]
                  privileges: [ read ]
                  query: '{"term" : {"field2" : "value2"}}'
                - names: [ 'fls-index' ]
                  privileges: [ read ]
                  field_security:
                     grant: [ 'field1', 'other_field', 'suggest_field2' ]
            role6:
              cluster: [ all ]
              indices:
                - names: '*'
                  privileges: [ ALL ]
                  query: '{"term" : {"color" : "red"}}'
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

    public void testSimpleQuery() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text"));
        prepareIndex("test").setId("1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("2").setSource("field2", "value2").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("3").setSource("field3", "value3").setRefreshPolicy(IMMEDIATE).get();

        assertSearchHitsWithoutFailures(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(randomBoolean() ? QueryBuilders.termQuery("field1", "value1") : QueryBuilders.matchAllQuery()),
            "1"
        );
        assertSearchHitsWithoutFailures(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(randomBoolean() ? QueryBuilders.termQuery("field2", "value2") : QueryBuilders.matchAllQuery()),
            "2"
        );
        QueryBuilder combined = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("field2", "value2"))
            .should(QueryBuilders.termQuery("field1", "value1"))
            .minimumShouldMatch(1);
        assertSearchHitsWithoutFailures(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(randomBoolean() ? combined : QueryBuilders.matchAllQuery()),
            "1",
            "2"
        );
    }

    public void testGetApi() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text"));

        prepareIndex("test").setId("1").setSource("field1", "value1").get();
        prepareIndex("test").setId("2").setSource("field2", "value2").get();
        prepareIndex("test").setId("3").setSource("field3", "value3").get();

        // test documents users can see
        boolean realtime = randomBoolean();
        GetResponse response = client().filterWithHeader(
            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
        ).prepareGet("test", "1").setRealtime(realtime).setRefresh(true).get();
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
            .prepareGet("test", "1")
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

    public void testRealtimeGetApi() {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text")
                .setSettings(Settings.builder().put("refresh_interval", "-1").build())
        );
        final boolean realtime = true;
        final boolean refresh = false;

        prepareIndex("test").setId("1").setSource("field1", "value1").get();
        prepareIndex("test").setId("2").setSource("field2", "value2").get();
        // do a realtime get beforehand to flip an internal translog flag so that subsequent realtime gets are
        // served from the translog (this first one is NOT, it internally forces a refresh of the index)
        client().prepareMultiGet().add("test", "1").add("test", "2").setRealtime(realtime).setRefresh(refresh).get();
        refresh("test");
        // updates don't change the doc visibility for users
        // but updates populate the translog and the DLS filter must apply to the translog operations as well
        if (randomBoolean()) {
            prepareIndex("test").setId("1")
                .setSource("field1", "value1", "field3", "value3")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.NONE)
                .get();
            prepareIndex("test").setId("2")
                .setSource("field2", "value2", "field3", "value3")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.NONE)
                .get();
        } else {
            client().prepareUpdate("test", "1").setDoc(Map.of("field3", "value3")).setRefreshPolicy(WriteRequest.RefreshPolicy.NONE).get();
            client().prepareUpdate("test", "2").setDoc(Map.of("field3", "value3")).setRefreshPolicy(WriteRequest.RefreshPolicy.NONE).get();
        }

        GetResponse getResponse;
        MultiGetResponse mgetResponse;
        // test documents user1 cannot see
        if (randomBoolean()) {
            getResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
            ).prepareGet("test", "2").setRealtime(realtime).setRefresh(refresh).get();
            assertThat(getResponse.isExists(), is(false));
        } else {
            mgetResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
            ).prepareMultiGet().add("test", "2").setRealtime(realtime).setRefresh(refresh).get();
            assertThat(mgetResponse.getResponses()[0].getResponse().isExists(), is(false));
        }
        // test documents user2 cannot see
        if (randomBoolean()) {
            getResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
            ).prepareGet("test", "1").setRealtime(realtime).setRefresh(refresh).get();
            assertThat(getResponse.isExists(), is(false));
        } else {
            mgetResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
            ).prepareMultiGet().add("test", "1").setRealtime(realtime).setRefresh(refresh).get();
            assertThat(mgetResponse.getResponses()[0].getResponse().isExists(), is(false));
        }
        // test visible documents are still visible after updates
        if (randomBoolean()) {
            getResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
            ).prepareGet("test", "1").setRealtime(realtime).setRefresh(refresh).get();
            assertThat(getResponse.isExists(), is(true));
        } else {
            getResponse = client().filterWithHeader(
                Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD))
            ).prepareGet("test", "2").setRealtime(realtime).setRefresh(refresh).get();
            assertThat(getResponse.isExists(), is(true));
        }
    }

    public void testMGetApi() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text"));

        prepareIndex("test").setId("1").setSource("field1", "value1").get();
        prepareIndex("test").setId("2").setSource("field2", "value2").get();
        prepareIndex("test").setId("3").setSource("field3", "value3").get();

        boolean realtime = randomBoolean();
        MultiGetResponse response = client().filterWithHeader(
            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
        ).prepareMultiGet().add("test", "1").setRealtime(realtime).setRefresh(true).get();
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
        assertAcked(
            indicesAdmin().prepareCreate("test1")
                .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text", "id", "type=integer")
        );
        assertAcked(
            indicesAdmin().prepareCreate("test2")
                .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text", "id", "type=integer")
        );

        prepareIndex("test1").setId("1").setSource("field1", "value1", "id", 1).get();
        prepareIndex("test1").setId("2").setSource("field2", "value2", "id", 2).get();
        prepareIndex("test1").setId("3").setSource("field3", "value3", "id", 3).get();
        prepareIndex("test2").setId("1").setSource("field1", "value1", "id", 1).get();
        prepareIndex("test2").setId("2").setSource("field2", "value2", "id", 2).get();
        prepareIndex("test2").setId("3").setSource("field3", "value3", "id", 3).get();
        indicesAdmin().prepareRefresh("test1", "test2").get();

        {
            assertResponse(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .prepareMultiSearch()
                    .add(prepareSearch("test1").setQuery(QueryBuilders.matchAllQuery()))
                    .add(prepareSearch("test2").setQuery(QueryBuilders.matchAllQuery())),
                response -> {
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
                }
            );
        }
        {
            assertResponse(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                    .prepareMultiSearch()
                    .add(prepareSearch("test1").setQuery(QueryBuilders.matchAllQuery()))
                    .add(prepareSearch("test2").setQuery(QueryBuilders.matchAllQuery())),
                response -> {
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
                }
            );
        }
        {
            assertResponse(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                    .prepareMultiSearch()
                    .add(
                        prepareSearch("test1").addSort(SortBuilders.fieldSort("id").sortMode(SortMode.MIN))
                            .setQuery(QueryBuilders.matchAllQuery())
                    )
                    .add(
                        prepareSearch("test2").addSort(SortBuilders.fieldSort("id").sortMode(SortMode.MIN))
                            .setQuery(QueryBuilders.matchAllQuery())
                    ),
                response -> {
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
            );
        }
    }

    public void testPercolateQueryWithIndexedDocWithDLS() {
        assertAcked(
            indicesAdmin().prepareCreate("query_index")
                .setMapping("message", "type=text", "query", "type=percolator", "field1", "type=text", "field2", "type=text")
        );
        assertAcked(indicesAdmin().prepareCreate("doc_index").setMapping("message", "type=text", "field1", "type=text"));
        prepareIndex("query_index").setId("1")
            .setSource("""
                {"field1": "value1", "field2": "value2", "query": {"match": {"message": "bonsai tree"}}}""", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex("doc_index").setId("1")
            .setSource("{\"field1\": \"value1\", \"message\": \"A new bonsai tree in the office\"}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE)
            .get();
        // user1 can preform the percolate search for doc#1 in the doc_index because user1 has access to the doc
        assertHitCountAndNoFailures(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("query_index")
                .setQuery(new PercolateQueryBuilder("query", "doc_index", "1", null, null, null)),
            1
        );
        // user2 can access the query_index itself (without performing percolate search)
        assertHitCountAndNoFailures(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("query_index")
                .setQuery(QueryBuilders.matchAllQuery()),
            1
        );
        // user2 cannot access doc#1 of the doc_index so the percolate search fails because doc#1 cannot be found
        ResourceNotFoundException e = expectThrows(
            ResourceNotFoundException.class,
            () -> client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("query_index")
                .setQuery(new PercolateQueryBuilder("query", "doc_index", "1", null, null, null))
                .get()
        );
        assertThat(e.getMessage(), is("indexed document [doc_index/1] couldn't be found"));
    }

    public void testGeoQueryWithIndexedShapeWithDLS() {
        assertAcked(
            indicesAdmin().prepareCreate("search_index")
                .setMapping("search_field", "type=shape", "field1", "type=text", "field2", "type=text")
        );
        assertAcked(
            indicesAdmin().prepareCreate("shape_index")
                .setMapping("shape_field", "type=shape", "field1", "type=text", "field2", "type=text")
        );
        prepareIndex("search_index").setId("1")
            .setSource("""
                {"field1": "value1", "field2": "value2", "search_field": { "type": "point", "coordinates":[1, 1] }}""", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex("shape_index").setId("1")
            .setSource("""
                {"field1": "value1", "shape_field": { "type": "envelope", "coordinates": [[0, 2], [2, 0]]}}""", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE)
            .get();
        ShapeQueryBuilder shapeQuery = new ShapeQueryBuilder("search_field", "1").relation(ShapeRelation.WITHIN)
            .indexedShapeIndex("shape_index")
            .indexedShapePath("shape_field");
        // user1 has access to doc#1 of the shape_index so everything works
        SearchRequestBuilder requestBuilder = client().filterWithHeader(
            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
        ).prepareSearch("search_index");
        if (randomBoolean()) {
            requestBuilder.setQuery(QueryBuilders.matchAllQuery()).setPostFilter(shapeQuery);
        } else {
            requestBuilder.setQuery(shapeQuery);
        }
        assertHitCountAndNoFailures(requestBuilder, 1);
        // user2 does not have access to doc#1 of the shape_index
        assertHitCountAndNoFailures(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("search_index")
                .setQuery(QueryBuilders.matchAllQuery()),
            1
        );
        IllegalArgumentException e;
        if (randomBoolean()) {
            e = expectThrows(
                IllegalArgumentException.class,
                () -> client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                    .prepareSearch("search_index")
                    .setQuery(shapeQuery)
                    .get()
            );
        } else {
            e = expectThrows(
                IllegalArgumentException.class,
                () -> client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                    .prepareSearch("search_index")
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setPostFilter(shapeQuery)
                    .get()
            );
        }
        assertThat(e.getMessage(), is("Shape with ID [1] not found"));
    }

    public void testTermsLookupOnIndexWithDLS() {
        assertAcked(
            indicesAdmin().prepareCreate("search_index")
                .setMapping("search_field", "type=keyword", "field1", "type=text", "field2", "type=text")
        );
        assertAcked(
            indicesAdmin().prepareCreate("lookup_index")
                .setMapping("lookup_field", "type=keyword", "field1", "type=text", "field2", "type=text")
        );
        prepareIndex("search_index").setId("1")
            .setSource("field1", "value1", "search_field", List.of("value1", "value2", "value3"))
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex("search_index").setId("2")
            .setSource("field1", "value1", "field2", "value2", "search_field", List.of("value1", "value2"))
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex("search_index").setId("3")
            .setSource("field1", "value1", "field2", "value1", "search_field", "value1")
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex("search_index").setId("4").setSource("field2", "value2", "search_field", "value1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("search_index").setId("5")
            .setSource("field2", "value2", "search_field", List.of("value1", "value2"))
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex("lookup_index").setId("1")
            .setSource("field1", "value1", "field2", "value1", "lookup_field", List.of("value1", "value2"))
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex("lookup_index").setId("2")
            .setSource("field1", "value2", "field2", "value2", "lookup_field", List.of("value2"))
            .setRefreshPolicy(IMMEDIATE)
            .get();

        // Lookup doc#1 is: visible to user1 and user3, but hidden from user2
        TermsQueryBuilder lookup = QueryBuilders.termsLookupQuery("search_field", new TermsLookup("lookup_index", "1", "lookup_field"));
        assertSearchHitsWithoutFailures(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("search_index")
                .setQuery(lookup),
            "1",
            "2",
            "3"
        );

        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("search_index")
                .setQuery(lookup),
            0
        );
        assertSearchHitsWithoutFailures(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("search_index")
                .setQuery(lookup),
            "1",
            "2",
            "3",
            "4",
            "5"
        );
        // Lookup doc#2 is: hidden from user1, visible to user2 and user3
        lookup = QueryBuilders.termsLookupQuery("search_field", new TermsLookup("lookup_index", "2", "lookup_field"));
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("search_index")
                .setQuery(lookup),
            0
        );
        assertSearchHitsWithoutFailures(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("search_index")
                .setQuery(lookup),
            "2",
            "5"
        );
        assertSearchHitsWithoutFailures(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("search_index")
                .setQuery(lookup),
            "1",
            "2",
            "5"
        );
    }

    public void testTVApi() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping(
                    "field1",
                    "type=text,term_vector=with_positions_offsets_payloads",
                    "field2",
                    "type=text,term_vector=with_positions_offsets_payloads",
                    "field3",
                    "type=text,term_vector=with_positions_offsets_payloads"
                )
        );
        prepareIndex("test").setId("1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("2").setSource("field2", "value2").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("3").setSource("field3", "value3").setRefreshPolicy(IMMEDIATE).get();

        boolean realtime = randomBoolean();
        TermVectorsResponse response = client().filterWithHeader(
            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
        ).prepareTermVectors("test", "1").setRealtime(realtime).get();
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
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping(
                    "field1",
                    "type=text,term_vector=with_positions_offsets_payloads",
                    "field2",
                    "type=text,term_vector=with_positions_offsets_payloads",
                    "field3",
                    "type=text,term_vector=with_positions_offsets_payloads"
                )
        );
        prepareIndex("test").setId("1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("2").setSource("field2", "value2").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("3").setSource("field3", "value3").setRefreshPolicy(IMMEDIATE).get();

        boolean realtime = randomBoolean();
        MultiTermVectorsResponse response = client().filterWithHeader(
            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
        ).prepareMultiTermVectors().add(new TermVectorsRequest("test", "1").realtime(realtime)).get();
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
            .add(new TermVectorsRequest("test", "1").realtime(realtime))
            .add(new TermVectorsRequest("test", "2").realtime(realtime))
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

    public void testKnnSearch() throws Exception {
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build();
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", 3)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .endObject()
            .endObject();
        assertAcked(indicesAdmin().prepareCreate("test").setSettings(indexSettings).setMapping(builder));

        for (int i = 0; i < 5; i++) {
            prepareIndex("test").setSource("field1", "value1", "other", "valueA", "vector", new float[] { i, i, i }).get();
            prepareIndex("test").setSource("field2", "value2", "other", "valueB", "vector", new float[] { i, i, i }).get();
        }

        indicesAdmin().prepareRefresh("test").get();

        // Since there's no kNN search action at the transport layer, we just emulate
        // how the action works (it builds a kNN query under the hood)
        float[] queryVector = new float[] { 0.0f, 0.0f, 0.0f };
        KnnVectorQueryBuilder query = new KnnVectorQueryBuilder("vector", queryVector, 50, null);

        if (randomBoolean()) {
            query.addFilterQuery(new WildcardQueryBuilder("other", "value*"));
        }

        // user1 should only be able to see docs with field1: value1
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(query)
                .addFetchField("field1")
                .setSize(10),
            response -> {
                assertEquals(5, response.getHits().getTotalHits().value);
                assertEquals(5, response.getHits().getHits().length);
                for (SearchHit hit : response.getHits().getHits()) {
                    assertNotNull(hit.field("field1"));
                }
            }
        );

        // user2 should only be able to see docs with field2: value2
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(query)
                .addFetchField("field2")
                .setSize(10),
            response -> {
                assertEquals(5, response.getHits().getTotalHits().value);
                assertEquals(5, response.getHits().getHits().length);
                for (SearchHit hit : response.getHits().getHits()) {
                    assertNotNull(hit.field("field2"));
                }
            }
        );

        // user3 can see all indexed docs
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(query)
                .setSize(10),
            response -> {
                assertEquals(10, response.getHits().getTotalHits().value);
                assertEquals(10, response.getHits().getHits().length);
            }
        );
    }

    public void testGlobalAggregation() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping("field1", "type=text", "field2", "type=text,fielddata=true", "field3", "type=text")
        );
        prepareIndex("test").setId("1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("2").setSource("field2", "value2").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("3").setSource("field3", "value3").setRefreshPolicy(IMMEDIATE).get();

        assertResponse(
            prepareSearch("test").addAggregation(
                AggregationBuilders.global("global").subAggregation(AggregationBuilders.terms("field2").field("field2"))
            ),
            response -> {
                assertHitCount(response, 3);
                assertSearchHits(response, "1", "2", "3");

                Global globalAgg = response.getAggregations().get("global");
                assertThat(globalAgg.getDocCount(), equalTo(3L));
                Terms termsAgg = globalAgg.getAggregations().get("field2");
                assertThat(termsAgg.getBuckets().get(0).getKeyAsString(), equalTo("value2"));
                assertThat(termsAgg.getBuckets().get(0).getDocCount(), equalTo(1L));
            }
        );
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .addAggregation(AggregationBuilders.global("global").subAggregation(AggregationBuilders.terms("field2").field("field2"))),
            response -> {
                assertHitCount(response, 1);
                assertSearchHits(response, "1");

                Global globalAgg = response.getAggregations().get("global");
                assertThat(globalAgg.getDocCount(), equalTo(1L));
                Terms termsAgg = globalAgg.getAggregations().get("field2");
                assertThat(termsAgg.getBuckets().size(), equalTo(0));
            }
        );
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .addAggregation(AggregationBuilders.global("global").subAggregation(AggregationBuilders.terms("field2").field("field2"))),
            response -> {
                assertHitCount(response, 1);
                assertSearchHits(response, "2");

                Global globalAgg = response.getAggregations().get("global");
                assertThat(globalAgg.getDocCount(), equalTo(1L));
                Terms termsAgg = globalAgg.getAggregations().get("field2");
                assertThat(termsAgg.getBuckets().size(), equalTo(1));
            }
        );
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .addAggregation(AggregationBuilders.global("global").subAggregation(AggregationBuilders.terms("field2").field("field2"))),
            response -> {
                assertHitCount(response, 2);
                assertSearchHits(response, "1", "2");

                Global globalAgg = response.getAggregations().get("global");
                assertThat(globalAgg.getDocCount(), equalTo(2L));
                Terms termsAgg = globalAgg.getAggregations().get("field2");
                assertThat(termsAgg.getBuckets().size(), equalTo(1));
            }
        );
    }

    public void testZeroMinDocAggregation() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setMapping("color", "type=keyword", "fruit", "type=keyword", "count", "type=integer")
                .setSettings(Map.of("index.number_of_shards", 1))
        );
        prepareIndex("test").setId("1").setSource("color", "red", "fruit", "apple", "count", -1).setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("2").setSource("color", "yellow", "fruit", "banana", "count", -2).setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("3").setSource("color", "green", "fruit", "grape", "count", -3).setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("test").setId("4").setSource("color", "red", "fruit", "grape", "count", -4).setRefreshPolicy(IMMEDIATE).get();
        indicesAdmin().prepareForceMerge("test").get();

        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user6", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(termQuery("fruit", "apple"))
                // global ordinal
                .addAggregation(AggregationBuilders.terms("colors1").field("color").minDocCount(0))
                .addAggregation(AggregationBuilders.terms("fruits").field("fruit").minDocCount(0))
                // global ordinal remapped
                .addAggregation(
                    AggregationBuilders.terms("colors2")
                        .field("color")
                        .minDocCount(0)
                        .includeExclude(new IncludeExclude(".*", null, null, null))
                )
                // mapped
                .addAggregation(AggregationBuilders.terms("colors3").field("color").minDocCount(0).executionHint("map"))
                // numeric
                .addAggregation(AggregationBuilders.terms("counts").field("count").minDocCount(0))
                // nested
                .addAggregation(
                    AggregationBuilders.terms("nested")
                        .field("color")
                        .minDocCount(0)
                        .subAggregation(
                            AggregationBuilders.terms("fruits")
                                .field("fruit")
                                .minDocCount(0)
                                .executionHint("map")
                                .subAggregation(AggregationBuilders.terms("counts").field("count").minDocCount(0))
                        )
                        .minDocCount(0)
                ),
            response -> {
                assertThat(
                    response.toString(),
                    allOf(
                        containsString("apple"),
                        containsString("grape"),
                        containsString("red"),
                        containsString("-1"),
                        containsString("-4")
                    )
                );
                assertThat(
                    response.toString(),
                    allOf(
                        not(containsString("banana")),
                        not(containsString("yellow")),
                        not(containsString("green")),
                        not(containsString("-2")),
                        not(containsString("-3"))
                    )
                );
                assertHitCount(response, 1);
                assertSearchHits(response, "1");
                // fruits
                StringTerms fruits = response.getAggregations().get("fruits");
                assertThat(fruits.getBuckets().size(), equalTo(2));
                List<StringTerms.Bucket> fruitBuckets = fruits.getBuckets();
                assertTrue(fruitBuckets.stream().anyMatch(bucket -> bucket.getKeyAsString().equals("apple") && bucket.getDocCount() == 1));
                assertTrue(fruitBuckets.stream().anyMatch(bucket -> bucket.getKeyAsString().equals("grape") && bucket.getDocCount() == 0));
                // counts
                LongTerms counts = response.getAggregations().get("counts");
                assertThat(counts.getBuckets().size(), equalTo(2));
                List<LongTerms.Bucket> countsBuckets = counts.getBuckets();
                assertTrue(countsBuckets.stream().anyMatch(bucket -> bucket.getKeyAsString().equals("-1") && bucket.getDocCount() == 1));
                assertTrue(countsBuckets.stream().anyMatch(bucket -> bucket.getKeyAsString().equals("-4") && bucket.getDocCount() == 0));
                // colors
                for (int i = 1; i <= 3; i++) {
                    StringTerms colors = response.getAggregations().get("colors" + i);
                    assertThat(colors.getBuckets().size(), equalTo(1));
                    assertThat(colors.getBuckets().get(0).getKeyAsString(), equalTo("red"));
                    assertThat(colors.getBuckets().get(0).getDocCount(), equalTo(1L));
                }
                // nested
                StringTerms nested = response.getAggregations().get("nested");
                assertThat(nested.getBuckets().size(), equalTo(1));
                assertThat(nested.getBuckets().get(0).getKeyAsString(), equalTo("red"));
                assertThat(nested.getBuckets().get(0).getDocCount(), equalTo(1L));
                StringTerms innerFruits = nested.getBuckets().get(0).getAggregations().get("fruits");
                List<StringTerms.Bucket> innerFruitsBuckets = innerFruits.getBuckets();
                assertTrue(innerFruitsBuckets.stream().anyMatch(b -> b.getKeyAsString().equals("apple") && b.getDocCount() == 1));
                assertTrue(innerFruitsBuckets.stream().anyMatch(b -> b.getKeyAsString().equals("grape") && b.getDocCount() == 0));
                assertThat(innerFruitsBuckets.size(), equalTo(2));

                for (int i = 0; i <= 1; i++) {
                    String parentBucketKey = innerFruitsBuckets.get(i).getKeyAsString();
                    LongTerms innerCounts = innerFruitsBuckets.get(i).getAggregations().get("counts");
                    assertThat(innerCounts.getBuckets().size(), equalTo(2));
                    List<LongTerms.Bucket> icb = innerCounts.getBuckets();
                    if ("apple".equals(parentBucketKey)) {
                        assertTrue(icb.stream().anyMatch(bucket -> bucket.getKeyAsString().equals("-1") && bucket.getDocCount() == 1));
                    } else {
                        assertTrue(icb.stream().anyMatch(bucket -> bucket.getKeyAsString().equals("-1") && bucket.getDocCount() == 0));
                    }
                    assertTrue(icb.stream().anyMatch(bucket -> bucket.getKeyAsString().equals("-4") && bucket.getDocCount() == 0));
                }
            }
        );
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
        assertAcked(prepareCreate("test").setMapping(mapping));
        ensureGreen();

        // index simple data
        prepareIndex("test").setId("p1").setSource("join_field", "parent", "field1", "value1").get();

        Map<String, Object> source = new HashMap<>();
        source.put("field2", "value2");
        source.put("id", "c1");
        Map<String, Object> joinField = new HashMap<>();
        joinField.put("name", "child");
        joinField.put("parent", "p1");
        source.put("join_field", joinField);
        prepareIndex("test").setId("c1").setSource(source).setRouting("p1").get();
        source.put("id", "c2");
        prepareIndex("test").setId("c2").setSource(source).setRouting("p1").get();
        source = new HashMap<>();
        source.put("field3", "value3");
        source.put("join_field", joinField);
        source.put("id", "c3");
        prepareIndex("test").setId("c3").setSource(source).setRouting("p1").get();
        refresh();
        verifyParentChild();
    }

    private void verifyParentChild() {
        assertResponse(prepareSearch("test").setQuery(hasChildQuery("child", matchAllQuery(), ScoreMode.None)), searchResponse -> {
            assertHitCount(searchResponse, 1L);
            assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("p1"));
        });

        assertResponse(
            prepareSearch("test").setQuery(hasParentQuery("parent", matchAllQuery(), false)).addSort("id", SortOrder.ASC),
            searchResponse -> {
                assertHitCount(searchResponse, 3L);
                assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("c1"));
                assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("c2"));
                assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("c3"));
            }
        );

        // Both user1 and user2 can't see field1 and field2, no parent/child query should yield results:
        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasChildQuery("child", matchAllQuery(), ScoreMode.None)),
            0L
        );

        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasChildQuery("child", matchAllQuery(), ScoreMode.None)),
            0L
        );

        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasParentQuery("parent", matchAllQuery(), false)),
            0L
        );

        assertHitCount(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasParentQuery("parent", matchAllQuery(), false)),
            0L
        );

        // user 3 can see them but not c3
        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasChildQuery("child", matchAllQuery(), ScoreMode.None)),
            searchResponse -> {
                assertHitCount(searchResponse, 1L);
                assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("p1"));
            }
        );

        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(hasParentQuery("parent", matchAllQuery(), false)),
            searchResponse -> {
                assertHitCount(searchResponse, 2L);
                assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("c1"));
                assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("c2"));
            }
        );
    }

    public void testScroll() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(Settings.builder().put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true))
                .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        final int numVisible = scaledRandomIntBetween(2, 10);
        final int numInVisible = scaledRandomIntBetween(2, 10);
        int id = 1;
        for (int i = 0; i < numVisible; i++) {
            prepareIndex("test").setId(String.valueOf(id++)).setSource("field1", "value1").get();
        }

        for (int i = 0; i < numInVisible; i++) {
            prepareIndex("test").setId(String.valueOf(id++)).setSource("field2", "value2").get();
            prepareIndex("test").setId(String.valueOf(id++)).setSource("field3", "value3").get();
        }
        refresh();

        SearchResponse response = null;
        try {
            response = client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
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

                response.decRef();
                response = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                ).prepareSearchScroll(response.getScrollId()).setScroll(TimeValue.timeValueMinutes(1L)).get();
            } while (response.getHits().getHits().length > 0);
        } finally {
            if (response != null) {
                String scrollId = response.getScrollId();
                response.decRef();
                if (scrollId != null) {
                    client().prepareClearScroll().addScrollId(scrollId).get();
                }
            }
        }
    }

    public void testReaderId() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(Settings.builder().put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true))
                .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        final int numVisible = scaledRandomIntBetween(2, 10);
        final int numInvisible = scaledRandomIntBetween(2, 10);
        int id = 1;
        for (int i = 0; i < numVisible; i++) {
            prepareIndex("test").setId(String.valueOf(id++)).setSource("field1", "value1").get();
        }

        for (int i = 0; i < numInvisible; i++) {
            prepareIndex("test").setId(String.valueOf(id++)).setSource("field2", "value2").get();
            prepareIndex("test").setId(String.valueOf(id++)).setSource("field3", "value3").get();
        }
        refresh();

        String pitId = openPointInTime("user1", TimeValue.timeValueMinutes(1), "test");
        SearchResponse response = null;
        try {
            for (int from = 0; from < numVisible; from++) {
                if (response != null) {
                    response.decRef();
                }
                response = client().filterWithHeader(
                    Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
                )
                    .prepareSearch()
                    .setSize(1)
                    .setFrom(from)
                    .setPointInTime(new PointInTimeBuilder(pitId))
                    .setQuery(termQuery("field1", "value1"))
                    .get();
                assertNoFailures(response);
                assertThat(response.getHits().getTotalHits().value, is((long) numVisible));
                assertThat(response.getHits().getAt(0).getSourceAsMap().size(), is(1));
                assertThat(response.getHits().getAt(0).getSourceAsMap().get("field1"), is("value1"));
            }
        } finally {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(response.pointInTimeId())).actionGet();
            response.decRef();
        }
    }

    public void testRequestCache() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(Settings.builder().put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true))
                .setMapping("field1", "type=text", "field2", "type=text", "field3", "type=text")
        );
        prepareIndex("test").setId("1").setSource("field1", "value1").get();
        prepareIndex("test").setId("2").setSource("field2", "value2").get();
        prepareIndex("test").setId("3").setSource("field3", "value3").get();
        refresh();

        int max = scaledRandomIntBetween(4, 32);
        for (int i = 0; i < max; i++) {
            Boolean requestCache = randomFrom(true, null);
            assertHitCountAndNoFailures(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                    .prepareSearch("test")
                    .setSize(0)
                    .setQuery(termQuery("field1", "value1"))
                    .setRequestCache(requestCache),
                1
            );
            assertHitCountAndNoFailures(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user2", USERS_PASSWD)))
                    .prepareSearch("test")
                    .setSize(0)
                    .setQuery(termQuery("field1", "value1"))
                    .setRequestCache(requestCache),
                0
            );
            assertHitCountAndNoFailures(
                client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user3", USERS_PASSWD)))
                    .prepareSearch("test")
                    .setSize(0)
                    .setQuery(termQuery("field1", "value1"))
                    .setRequestCache(requestCache),
                1
            );
        }
    }

    public void testUpdateApiIsBlocked() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field1", "type=text", "field2", "type=text"));
        prepareIndex("test").setId("1").setSource("field1", "value1").setRefreshPolicy(IMMEDIATE).get();

        // With document level security enabled the update is not allowed:
        try {
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD)))
                .prepareUpdate("test", "1")
                .setDoc(Requests.INDEX_CONTENT_TYPE, "field1", "value2")
                .get();
            fail("failed, because update request shouldn't be allowed if document level security is enabled");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(e.getMessage(), equalTo("Can't execute an update request if field or document level security is enabled"));
        }
        assertThat(client().prepareGet("test", "1").get().getSource().get("field1").toString(), equalTo("value1"));

        // With no document level security enabled the update is allowed:
        client().prepareUpdate("test", "1").setDoc(Requests.INDEX_CONTENT_TYPE, "field1", "value2").get();
        assertThat(client().prepareGet("test", "1").get().getSource().get("field1").toString(), equalTo("value2"));

        // With document level security enabled the update in bulk is not allowed:
        BulkResponse bulkResponse = client().filterWithHeader(
            Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user1", USERS_PASSWD))
        ).prepareBulk().add(new UpdateRequest("test", "1").doc(Requests.INDEX_CONTENT_TYPE, "field1", "value3")).get();
        assertEquals(1, bulkResponse.getItems().length);
        BulkItemResponse bulkItem = bulkResponse.getItems()[0];
        assertTrue(bulkItem.isFailed());
        assertThat(bulkItem.getFailure().getCause(), instanceOf(ElasticsearchSecurityException.class));
        ElasticsearchSecurityException securityException = (ElasticsearchSecurityException) bulkItem.getFailure().getCause();
        assertThat(securityException.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(
            securityException.getMessage(),
            equalTo("Can't execute a bulk item request with update requests embedded if field or document level security is enabled")
        );

        assertThat(client().prepareGet("test", "1").get().getSource().get("field1").toString(), equalTo("value2"));

        client().prepareBulk().add(new UpdateRequest("test", "1").doc(Requests.INDEX_CONTENT_TYPE, "field1", "value3")).get();
        assertThat(client().prepareGet("test", "1").get().getSource().get("field1").toString(), equalTo("value3"));
    }

    public void testNestedInnerHits() throws Exception {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("field1", "type=text", "nested_field", "type=nested"));
        prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", "value1")
                    .startArray("nested_field")
                    .startObject()
                    .field("field2", "value2")
                    .endObject()
                    .startObject()
                    .array("field2", "value2", "value3")
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        prepareIndex("test").setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", "value2")
                    .startArray("nested_field")
                    .startObject()
                    .field("field2", "value2")
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        refresh("test");

        assertResponse(
            client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user4", USERS_PASSWD)))
                .prepareSearch("test")
                .setQuery(
                    QueryBuilders.nestedQuery("nested_field", QueryBuilders.termQuery("nested_field.field2", "value2"), ScoreMode.None)
                        .innerHit(new InnerHitBuilder())
                ),
            response -> {
                assertHitCount(response, 1);
                assertSearchHits(response, "1");
                assertThat(response.getHits().getAt(0).getInnerHits().get("nested_field").getAt(0).getId(), equalTo("1"));
                assertThat(
                    response.getHits().getAt(0).getInnerHits().get("nested_field").getAt(0).getNestedIdentity().getOffset(),
                    equalTo(0)
                );
                assertThat(
                    response.getHits().getAt(0).getInnerHits().get("nested_field").getAt(0).getSourceAsString(),
                    equalTo("{\"field2\":\"value2\"}")
                );
                assertThat(
                    response.getHits().getAt(0).getInnerHits().get("nested_field").getAt(1).getNestedIdentity().getOffset(),
                    equalTo(1)
                );
                assertThat(
                    response.getHits().getAt(0).getInnerHits().get("nested_field").getAt(1).getSourceAsString(),
                    equalTo("{\"field2\":[\"value2\",\"value3\"]}")
                );
            }
        );
    }

    public void testSuggesters() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(indexSettings(1, 0))
                .setMapping("field1", "type=text", "suggest_field1", "type=text", "suggest_field2", "type=completion")
        );

        prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("field1", "value1")
                    .field("suggest_field1", "value")
                    .startObject("suggest_field2")
                    .field("input", "value")
                    .endObject()
                    .endObject()
            )
            .get();
        // A document that is always included by role query of both roles:
        prepareIndex("test").setId("2")
            .setSource(jsonBuilder().startObject().field("field1", "value1").field("field2", "value2").endObject())
            .get();
        refresh("test");

        assertAcked(
            indicesAdmin().prepareCreate("fls-index")
                .setSettings(indexSettings(1, 0))
                .setMapping(
                    "field1",
                    "type=text",
                    "suggest_field1",
                    "type=text",
                    "suggest_field2",
                    "type=completion",
                    "yet_another",
                    "type=text"
                )
        );

        // Term suggester:
        assertNoFailuresAndResponse(
            prepareSearch("test").suggest(
                new SuggestBuilder().setGlobalText("valeu").addSuggestion("_name1", new TermSuggestionBuilder("suggest_field1"))
            ),
            response -> {

                TermSuggestion termSuggestion = response.getSuggest().getSuggestion("_name1");
                assertThat(termSuggestion, notNullValue());
                assertThat(termSuggestion.getEntries().size(), equalTo(1));
                assertThat(termSuggestion.getEntries().get(0).getOptions().size(), equalTo(1));
                assertThat(termSuggestion.getEntries().get(0).getOptions().get(0).getText().string(), equalTo("value"));
            }
        );

        final String[] indices = randomFrom(
            List.of(new String[] { "test" }, new String[] { "fls-index", "test" }, new String[] { "test", "fls-index" })
        );

        Exception e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch(indices)
                .suggest(new SuggestBuilder().setGlobalText("valeu").addSuggestion("_name1", new TermSuggestionBuilder("suggest_field1")))
                .get()
        );
        assertThat(e.getMessage(), equalTo("Suggest isn't supported if document level security is enabled"));

        // Phrase suggester:
        assertNoFailuresAndResponse(
            prepareSearch("test").suggest(
                new SuggestBuilder().setGlobalText("valeu").addSuggestion("_name1", new PhraseSuggestionBuilder("suggest_field1"))
            ),
            response -> {

                PhraseSuggestion phraseSuggestion = response.getSuggest().getSuggestion("_name1");
                assertThat(phraseSuggestion, notNullValue());
                assertThat(phraseSuggestion.getEntries().size(), equalTo(1));
                assertThat(phraseSuggestion.getEntries().get(0).getOptions().size(), equalTo(1));
                assertThat(phraseSuggestion.getEntries().get(0).getOptions().get(0).getText().string(), equalTo("value"));
            }
        );
        e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch(indices)
                .suggest(new SuggestBuilder().setGlobalText("valeu").addSuggestion("_name1", new PhraseSuggestionBuilder("suggest_field1")))
                .get()
        );
        assertThat(e.getMessage(), equalTo("Suggest isn't supported if document level security is enabled"));

        // Completion suggester:
        assertNoFailuresAndResponse(
            prepareSearch("test").suggest(
                new SuggestBuilder().setGlobalText("valu").addSuggestion("_name1", new CompletionSuggestionBuilder("suggest_field2"))
            ),
            response -> {
                CompletionSuggestion completionSuggestion = response.getSuggest().getSuggestion("_name1");
                assertThat(completionSuggestion, notNullValue());
                assertThat(completionSuggestion.getEntries().size(), equalTo(1));
                assertThat(completionSuggestion.getEntries().get(0).getOptions().size(), equalTo(1));
                assertThat(completionSuggestion.getEntries().get(0).getOptions().get(0).getText().string(), equalTo("value"));
            }
        );

        e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch(indices)
                .suggest(
                    new SuggestBuilder().setGlobalText("valeu").addSuggestion("_name1", new CompletionSuggestionBuilder("suggest_field2"))
                )
                .get()
        );
        assertThat(e.getMessage(), equalTo("Suggest isn't supported if document level security is enabled"));
    }

    public void testProfile() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate("test")
                .setSettings(indexSettings(1, 0))
                .setMapping("field1", "type=text", "other_field", "type=text")
        );

        prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().field("field1", "value1").field("other_field", "value").endObject())
            .get();
        // A document that is always included by role query of both roles:
        prepareIndex("test").setId("2")
            .setSource(jsonBuilder().startObject().field("field1", "value1").field("field2", "value2").endObject())
            .get();
        refresh("test");

        assertAcked(
            indicesAdmin().prepareCreate("fls-index")
                .setSettings(indexSettings(1, 0))
                .setMapping("field1", "type=text", "other_field", "type=text", "yet_another", "type=text")
        );

        assertNoFailuresAndResponse(
            prepareSearch("test").setProfile(true).setQuery(new FuzzyQueryBuilder("other_field", "valeu")),
            response -> {
                assertThat(response.getProfileResults().size(), equalTo(1));
                SearchProfileShardResult shardResult = response.getProfileResults().get(response.getProfileResults().keySet().toArray()[0]);
                assertThat(shardResult.getQueryProfileResults().size(), equalTo(1));
                QueryProfileShardResult queryProfileShardResult = shardResult.getQueryProfileResults().get(0);
                assertThat(queryProfileShardResult.getQueryResults().size(), equalTo(1));
                logger.info("queryProfileShardResult=" + Strings.toString(queryProfileShardResult));
                assertThat(
                    queryProfileShardResult.getQueryResults().stream().map(ProfileResult::getLuceneDescription).sorted().collect(toList()),
                    equalTo(List.of("(other_field:value)^0.8"))
                );
            }
        );

        final String[] indices = randomFrom(
            List.of(new String[] { "test" }, new String[] { "fls-index", "test" }, new String[] { "test", "fls-index" })
        );
        Exception e = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue("user5", USERS_PASSWD)))
                .prepareSearch(indices)
                .setProfile(true)
                .setQuery(new FuzzyQueryBuilder("other_field", "valeu"))
                .get()
        );
        assertThat(e.getMessage(), equalTo("A search request cannot be profiled if document level security is enabled"));
    }

}
