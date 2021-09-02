/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.join;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.JoinHitLookupBuilder;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Before;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class LookupJoinHitsIT extends ESIntegTestCase {

    @Before
    public void populateIndex() throws Exception {
        client().admin().indices().prepareCreate("user")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .get();
        client().prepareBulk("users")
            .add(new IndexRequest().id("u1").source("device", "ios", "name", "John New York"))
            .add(new IndexRequest().id("u2").source("device", "android", "name", "Mike Boston"))
            .add(new IndexRequest().id("u3").source("device", "linux", "name", "Jack Austin"))
            .add(new IndexRequest().id("u4").source("device", "macos", "name", "Tony London"))
            .add(new IndexRequest().id("u5").source("device", "macos", "name", "Tim Tokyo"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        client().admin().indices().prepareCreate("calls")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .setMapping("from_user", "type=keyword", "to_user", "type=keyword")
            .get();
        client().prepareBulk("calls")
            .add(new IndexRequest().source("from_user", "u1", "to_user", "u2", "duration", 20))
            .add(new IndexRequest().source("from_user", "u2", "to_user", "u1", "duration", 25))
            .add(new IndexRequest().source("from_user", "u1", "to_user", List.of("u2", "u3"), "duration", 30))
            .add(new IndexRequest().source("from_user", "u3", "to_user", List.of("u1"), "duration", 35))
            .add(new IndexRequest().source("from_user", "u4", "to_user", List.of("u5", "u7"), "duration", 40))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    public void testBasic() throws Exception {
        final SearchResponse searchResponse = client().prepareSearch("calls")
            .setFetchSource(false)
            .setQuery(new TermQueryBuilder("from_user", "u1"))
            .addJoinHitLookup(new JoinHitLookupBuilder("from_user", "users", "from_user"))
            .addJoinHitLookup(new JoinHitLookupBuilder("to_user", "users", "to_user"))
            .addFetchField("from_user")
            .addFetchField("to_user")
            .addSort(new FieldSortBuilder("duration"))
            .get();
        ElasticsearchAssertions.assertHitCount(searchResponse, 2);
        {
            final SearchHit firstHit = searchResponse.getHits().getHits()[0];
            assertThat(firstHit.field("from_user").getValues(), equalTo(List.of("u1")));
            assertThat(firstHit.field("to_user").getValues(), equalTo(List.of("u2")));
            final List<SearchHit.JoinHit> fromUsers = firstHit.getJoinHits().get("from_user");
            assertNotNull(fromUsers);
            assertThat(fromUsers, hasSize(1));
            assertThat(fromUsers.get(0).getId(), equalTo("u1"));
            assertThat(SourceLookup.sourceAsMap(fromUsers.get(0).getSource()).get("name"), equalTo("John New York"));
            assertThat(SourceLookup.sourceAsMap(fromUsers.get(0).getSource()).get("device"), equalTo("ios"));

            final List<SearchHit.JoinHit> toUsers = firstHit.getJoinHits().get("to_user");
            assertNotNull(toUsers);
            assertThat(toUsers, hasSize(1));
            assertThat(toUsers.get(0).getId(), equalTo("u2"));
            assertThat(SourceLookup.sourceAsMap(toUsers.get(0).getSource()).get("name"), equalTo("Mike Boston"));
            assertThat(SourceLookup.sourceAsMap(toUsers.get(0).getSource()).get("device"), equalTo("android"));
        }
        {
            final SearchHit secondHit = searchResponse.getHits().getHits()[1];
            assertThat(secondHit.field("from_user").getValues(), equalTo(List.of("u1")));
            assertThat(secondHit.field("to_user").getValues(), equalTo(List.of("u2", "u3")));
            final List<SearchHit.JoinHit> fromUsers = secondHit.getJoinHits().get("from_user");
            assertNotNull(fromUsers);
            assertThat(fromUsers, hasSize(1));
            final SearchHit.JoinHit fromUser = fromUsers.get(0);
            assertThat(fromUser.getId(), equalTo("u1"));
            final Map<String, Object> sourceAsMap = SourceLookup.sourceAsMap(fromUser.getSource());
            assertThat(sourceAsMap.get("name"), equalTo("John New York"));
            assertThat(sourceAsMap.get("device"), equalTo("ios"));

            final List<SearchHit.JoinHit> toUsers = secondHit.getJoinHits().get("to_user");
            assertNotNull(toUsers);
            assertThat(toUsers, hasSize(2));
            toUsers.sort(Comparator.comparing(SearchHit.JoinHit::getId));
            assertThat(toUsers.get(0).getId(), equalTo("u2"));
            assertThat(SourceLookup.sourceAsMap(toUsers.get(0).getSource()).get("name"), equalTo("Mike Boston"));
            assertThat(SourceLookup.sourceAsMap(toUsers.get(0).getSource()).get("device"), equalTo("android"));

            assertThat(toUsers.get(1).getId(), equalTo("u3"));
            assertThat(SourceLookup.sourceAsMap(toUsers.get(1).getSource()).get("name"), equalTo("Jack Austin"));
            assertThat(SourceLookup.sourceAsMap(toUsers.get(1).getSource()).get("device"), equalTo("linux"));
        }
    }

    public void testIndexNotExist() throws Exception {
        final SearchResponse searchResponse = client().prepareSearch("calls")
            .setFetchSource(false)
            .setQuery(new TermQueryBuilder("from_user", "u3"))
            .addJoinHitLookup(new JoinHitLookupBuilder("from_user", "not_user_index", "from_user"))
            .addFetchField("from_user")
            .addSort(new FieldSortBuilder("duration"))
            .get();
        ElasticsearchAssertions.assertHitCount(searchResponse, 1);
        final SearchHit firstHit = searchResponse.getHits().getHits()[0];
        assertThat(firstHit.field("from_user").getValues(), equalTo(List.of("u3")));
        final List<SearchHit.JoinHit> fromUsers = firstHit.getJoinHits().get("from_user");
        assertNotNull(fromUsers);
        assertThat(fromUsers, hasSize(1));
        assertThat(fromUsers.get(0).getFailure(), instanceOf(IndexNotFoundException.class));
    }

    public void testMatchKeyDoesNotExist() throws Exception {
        final SearchResponse searchResponse = client().prepareSearch("calls")
            .setFetchSource(false)
            .setQuery(new TermQueryBuilder("from_user", "u4"))
            .addJoinHitLookup(new JoinHitLookupBuilder("from_user", "users", "from_user"))
            .addJoinHitLookup(new JoinHitLookupBuilder("to_user", "users", "to_user"))
            .addFetchField("from_user")
            .addFetchField("to_user")
            .addSort(new FieldSortBuilder("duration"))
            .get();
        ElasticsearchAssertions.assertHitCount(searchResponse, 1);
        final SearchHit firstHit = searchResponse.getHits().getHits()[0];
        assertThat(firstHit.field("from_user").getValues(), equalTo(List.of("u4")));
        assertThat(firstHit.field("to_user").getValues(), equalTo(List.of("u5", "u7")));
        final List<SearchHit.JoinHit> fromUsers = firstHit.getJoinHits().get("from_user");
        assertNotNull(fromUsers);
        assertThat(fromUsers, hasSize(1));
        final SearchHit.JoinHit fromUser = fromUsers.get(0);
        assertThat(fromUser.getId(), equalTo("u4"));
        final Map<String, Object> sourceAsMap = SourceLookup.sourceAsMap(fromUser.getSource());
        assertThat(sourceAsMap.get("name"), equalTo("Tony London"));
        assertThat(sourceAsMap.get("device"), equalTo("macos"));

        final List<SearchHit.JoinHit> toUsers = firstHit.getJoinHits().get("to_user");
        assertNotNull(toUsers);
        assertThat(toUsers, hasSize(2));
        toUsers.sort(Comparator.comparing(SearchHit.JoinHit::getId));
        assertThat(toUsers.get(0).getId(), equalTo("u5"));
        assertThat(SourceLookup.sourceAsMap(toUsers.get(0).getSource()).get("name"), equalTo("Tim Tokyo"));
        assertThat(SourceLookup.sourceAsMap(toUsers.get(0).getSource()).get("device"), equalTo("macos"));

        assertThat(toUsers.get(1).getId(), equalTo("u7"));
        assertThat(toUsers.get(1).getFailure(), instanceOf(ResourceNotFoundException.class));
    }

    public void testRejectIfClusterDoesNotAllowExpenseQueries() throws Exception {
        final String rejectNode = internalCluster().startNode(Settings.builder().put("search.allow_expensive_queries", false));
        final IllegalArgumentException error = expectThrows(IllegalArgumentException.class,
            () -> client(rejectNode).prepareSearch("calls")
                .addJoinHitLookup(new JoinHitLookupBuilder("from_user", "users", "from_user"))
                .get());
        assertThat(error.getMessage(),
            equalTo("query with [join_hits] cannot be executed when [search.allow_expensive_queries] is set to false."));
        internalCluster().stopNode(rejectNode);
    }
}
