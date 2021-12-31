/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.get.MultiGetShardRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.JoinField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class JoinFieldIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    @Before
    public void populateIndex() throws Exception {
        client().admin()
            .indices()
            .prepareCreate("authors")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .get();
        client().prepareBulk("authors")
            .add(new IndexRequest().id("john").source("first_name", "John", "last_name", "New York", "joined", 2010))
            .add(new IndexRequest().id("mike").source("first_name", "Mike", "last_name", "Boston", "joined", 2011))
            .add(new IndexRequest().id("jack").source("first_name", "Jack", "last_name", "Austin", "joined", 2012))
            .get();
        client().admin()
            .indices()
            .prepareCreate("publishers")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .get();
        client().prepareBulk("publishers")
            .add(new IndexRequest().id("p1").source("name", "The first publisher", "city", List.of("Montreal", "Vancouver")))
            .add(new IndexRequest().id("p2").source("name", "The second publisher", "city", "Toronto"))
            .get();
        client().admin()
            .indices()
            .prepareCreate("books")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .setMapping("title", "type=text", "author", "type=keyword", "published_date", "type=date,format=yyyy-MM-dd")
            .get();
        client().prepareBulk("books")
            .add(
                new IndexRequest().id("book_1")
                    .source("title", "the first book", "author_id", "john", "publisher_id", "p1", "published_date", "2020-01-05")
            )
            .add(
                new IndexRequest().id("book_2")
                    .source("title", "the second book", "author_id", "mike", "publisher_id", "p2", "published_date", "2020-02-10")
            )
            .add(
                new IndexRequest().id("book_3")
                    .source(
                        "title",
                        "the third book",
                        "author_id",
                        List.of("mike", "mark"),
                        "publisher_id",
                        "p1",
                        "published_date",
                        "2021-04-20"
                    )
            )
            .add(
                new IndexRequest().id("book_4")
                    .source(
                        "title",
                        "the forth book",
                        "author_id",
                        List.of("mike", "jack"),
                        "publisher_id",
                        "p1",
                        "published_date",
                        "2021-05-11"
                    )
            )
            .add(
                new IndexRequest().id("book_5")
                    .source("title", "the fifth book", "author_id", "mike", "publisher_id", "p2", "published_date", "2021-06-30")
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
    }

    public void testBasic() {
        SearchResponse searchResponse = client().prepareSearch("books")
            .setFetchSource(false)
            .addJoinField(new JoinField("author", "authors", "author_id", List.of("first_name", "last_name")))
            .addFetchField("title")
            .addFetchField("author_id")
            .addSort("published_date", SortOrder.DESC)
            .setSize(2)
            .get();
        ElasticsearchAssertions.assertHitCount(searchResponse, 5);
        SearchHit hit0 = searchResponse.getHits().getHits()[0];
        assertThat(hit0.field("author_id").getValues(), equalTo(List.of("mike")));
        assertThat(hit0.field("title").getValues(), equalTo(List.of("the fifth book")));
        assertThat(
            hit0.field("author").getValues(),
            contains(Map.of("_id", List.of("mike"), "first_name", List.of("Mike"), "last_name", List.of("Boston")))
        );

        SearchHit hit1 = searchResponse.getHits().getHits()[1];
        assertThat(hit1.field("author_id").getValues(), equalTo(List.of("mike", "jack")));
        assertThat(hit1.field("title").getValues(), equalTo(List.of("the forth book")));
        assertThat(
            hit1.field("author").getValues(),
            containsInAnyOrder(
                Map.of("_id", List.of("mike"), "first_name", List.of("Mike"), "last_name", List.of("Boston")),
                Map.of("_id", List.of("jack"), "first_name", List.of("Jack"), "last_name", List.of("Austin"))
            )
        );
    }

    public void testJoinMultipleIndices() {
        SearchResponse searchResponse = client().prepareSearch("books")
            .setFetchSource(false)
            .addJoinField(new JoinField("author", "authors", "author_id", List.of("first_name", "last_name")))
            .addJoinField(new JoinField("publisher", "publishers", "publisher_id", null))
            .addFetchField("title")
            .addFetchField("author_id")
            .addFetchField("publisher_id")
            .addSort("published_date", SortOrder.DESC)
            .setSize(2)
            .get();
        SearchHit hit0 = searchResponse.getHits().getHits()[0];
        assertThat(hit0.field("author_id").getValues(), equalTo(List.of("mike")));
        assertThat(hit0.field("title").getValues(), equalTo(List.of("the fifth book")));
        assertThat(
            hit0.field("author").getValues(),
            contains(Map.of("_id", List.of("mike"), "first_name", List.of("Mike"), "last_name", List.of("Boston")))
        );
        assertThat(
            hit0.field("publisher").getValues(),
            contains(Map.of("_id", List.of("p2"), "name", List.of("The second publisher"), "city", List.of("Toronto")))
        );

        SearchHit hit1 = searchResponse.getHits().getHits()[1];
        assertThat(hit1.field("author_id").getValues(), equalTo(List.of("mike", "jack")));
        assertThat(hit1.field("title").getValues(), equalTo(List.of("the forth book")));
        assertThat(
            hit1.field("author").getValues(),
            containsInAnyOrder(
                Map.of("_id", List.of("mike"), "first_name", List.of("Mike"), "last_name", List.of("Boston")),
                Map.of("_id", List.of("jack"), "first_name", List.of("Jack"), "last_name", List.of("Austin"))
            )
        );
        assertThat(
            hit1.field("publisher").getValues(),
            containsInAnyOrder(
                Map.of("_id", List.of("p1"), "name", List.of("The first publisher"), "city", List.of("Montreal", "Vancouver"))
            )
        );
    }

    public void testSourceFiltering() {
        SearchResponse searchResponse = client().prepareSearch("books")
            .setFetchSource(false)
            .addJoinField(new JoinField("author", "authors", "author_id", randomBoolean() ? List.of("*") : null))
            .addFetchField("title")
            .addFetchField("author_id")
            .addSort("published_date", SortOrder.DESC)
            .setSize(1)
            .get();
        SearchHit hit0 = searchResponse.getHits().getHits()[0];
        assertThat(hit0.field("author_id").getValues(), equalTo(List.of("mike")));
        assertThat(hit0.field("title").getValues(), equalTo(List.of("the fifth book")));
        assertThat(
            hit0.field("author").getValues(),
            contains(Map.of("_id", List.of("mike"), "first_name", List.of("Mike"), "last_name", List.of("Boston"), "joined", List.of(2011)))
        );
    }

    public void testIndexNotExist() {
        SearchResponse searchResponse = client().prepareSearch("books")
            .setFetchSource(false)
            .setQuery(new TermQueryBuilder("title", "second"))
            .addFetchField("title")
            .addFetchField("author_id")
            .addJoinField(new JoinField("author", "book_author", "author_id", List.of("first_name", "last_name")))
            .get();
        ElasticsearchAssertions.assertHitCount(searchResponse, 1);
        SearchHit hit0 = searchResponse.getHits().getHits()[0];
        assertThat(hit0.field("author_id").getValues(), equalTo(List.of("mike")));
        assertThat(hit0.field("title").getValues(), equalTo(List.of("the second book")));
        assertThat(
            hit0.field("author").getValues(),
            contains(Map.of("_id", List.of("mike"), "_failure", List.of("no such index [book_author]")))
        );
    }

    public void testMatchKeyDoesNotExist() {
        SearchResponse searchResponse = client().prepareSearch("books")
            .setFetchSource(false)
            .setQuery(new TermQueryBuilder("title", "third"))
            .addFetchField("title")
            .addFetchField("author_id")
            .addJoinField(new JoinField("author", "authors", "author_id", List.of("first_name", "last_name")))
            .get();
        ElasticsearchAssertions.assertHitCount(searchResponse, 1);
        SearchHit hit0 = searchResponse.getHits().getHits()[0];
        assertThat(hit0.field("author_id").getValues(), containsInAnyOrder("mike", "mark"));
        assertThat(hit0.field("title").getValues(), equalTo(List.of("the third book")));
        assertThat(
            hit0.field("author").getValues(),
            containsInAnyOrder(
                Map.of("_id", List.of("mark"), "_failure", List.of("id [mark] on index [authors] doesn't exist")),
                Map.of("_id", List.of("mike"), "first_name", List.of("Mike"), "last_name", List.of("Boston"))
            )
        );
    }

    public void testExpensiveQueries() throws Exception {
        final String rejectNode = internalCluster().startNode(Settings.builder().put("search.allow_expensive_queries", false));
        final IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> client(rejectNode).prepareSearch("books")
                .addJoinField(new JoinField("author", "authors", "author", List.of("first_name", "last_name")))
                .addFetchField("author_id")
                .get()
        );
        assertThat(error.getMessage(), equalTo("query with join fields require [search.allow_expensive_queries] enabled"));
        internalCluster().stopNode(rejectNode);
    }

    public void testLookupLocallyOnDataNodes() throws Exception {
        ElasticsearchAssertions.assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("authors")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all"))
                .get()
        );
        ensureGreen("authors");
        final AtomicInteger lookupRequests = new AtomicInteger();
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.addRequestHandlingBehavior("indices:data/read/mget[shard][s]", (handler, request, channel, task) -> {
                assertThat(request, instanceOf(MultiGetShardRequest.class));
                assertThat(((MultiGetShardRequest) request).preference(), equalTo("_only_local"));
                assertThat(channel.getChannelType(), equalTo("direct"));
                lookupRequests.incrementAndGet();
                handler.messageReceived(request, channel, task);
            });
        }
        SearchResponse searchResponse = client().prepareSearch("books")
            .setSearchType(randomFrom(SearchType.values()))
            .setFetchSource(false)
            .addJoinField(new JoinField("author", "authors", "author_id", List.of("first_name", "last_name")))
            .addFetchField("title")
            .addFetchField("author_id")
            .addSort("published_date", SortOrder.DESC)
            .setSize(2)
            .get();
        assertThat(searchResponse.getHits().getHits(), arrayWithSize(2));
        SearchHit hit0 = searchResponse.getHits().getHits()[0];
        assertThat(hit0.field("author_id").getValues(), equalTo(List.of("mike")));
        assertThat(hit0.field("title").getValues(), equalTo(List.of("the fifth book")));
        assertThat(
            hit0.field("author").getValues(),
            contains(Map.of("_id", List.of("mike"), "first_name", List.of("Mike"), "last_name", List.of("Boston")))
        );

        SearchHit hit1 = searchResponse.getHits().getHits()[1];
        assertThat(hit1.field("author_id").getValues(), equalTo(List.of("mike", "jack")));
        assertThat(hit1.field("title").getValues(), equalTo(List.of("the forth book")));
        assertThat(
            hit1.field("author").getValues(),
            containsInAnyOrder(
                Map.of("_id", List.of("mike"), "first_name", List.of("Mike"), "last_name", List.of("Boston")),
                Map.of("_id", List.of("jack"), "first_name", List.of("Jack"), "last_name", List.of("Austin"))
            )
        );
        assertThat(lookupRequests.get(), greaterThan(0));
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.clearAllRules();
        }
    }
}
