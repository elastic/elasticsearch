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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

public class LookupRuntimeFieldIT extends ESIntegTestCase {

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
            .add(new IndexRequest().id("john").source("first_name", "John", "last_name", "New York"))
            .add(new IndexRequest().id("mike").source("first_name", "Mike", "last_name", "Boston"))
            .add(new IndexRequest().id("jack").source("first_name", "Jack", "last_name", "Austin"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        client().admin()
            .indices()
            .prepareCreate("publishers")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .get();
        client().prepareBulk("publishers")
            .add(new IndexRequest().id("p1").source("name", "The first publisher", "city", List.of("Montreal", "Vancouver")))
            .add(new IndexRequest().id("p2").source("name", "The second publisher", "city", "Toronto"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        client().admin()
            .indices()
            .prepareCreate("books")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .setMapping(
                Map.of(
                    "properties",
                    Map.of(
                        "title",
                        Map.of("type", "text"),
                        "author_id",
                        Map.of("type", "keyword"),
                        "published_date",
                        Map.of("type", "date", "format", "yyyy-MM-dd")
                    ),
                    "runtime",
                    Map.of(
                        "author",
                        Map.of(
                            "type",
                            "lookup",
                            "lookup_index",
                            "authors",
                            "id_field",
                            "author_id",
                            "lookup_fields",
                            List.of("first_name", "last_name")
                        )
                    )
                )
            )
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
                        List.of("mark", "mike"),
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
            .addFetchField("author")
            .addFetchField("title")
            .addSort("published_date", SortOrder.DESC)
            .setSize(3)
            .get();
        ElasticsearchAssertions.assertNoFailures(searchResponse);
        ElasticsearchAssertions.assertHitCount(searchResponse, 5);

        SearchHit hit0 = searchResponse.getHits().getHits()[0];
        assertThat(hit0.field("title").getValues(), equalTo(List.of("the fifth book")));
        assertThat(
            hit0.field("author").getValues(),
            equalTo(List.of(Map.of("_id", List.of("mike"), "first_name", List.of("Mike"), "last_name", List.of("Boston"))))
        );

        SearchHit hit1 = searchResponse.getHits().getHits()[1];
        assertThat(hit1.field("title").getValues(), equalTo(List.of("the forth book")));
        assertThat(
            hit1.field("author").getValues(),
            equalTo(
                List.of(
                    Map.of("_id", List.of("mike"), "first_name", List.of("Mike"), "last_name", List.of("Boston")),
                    Map.of("_id", List.of("jack"), "first_name", List.of("Jack"), "last_name", List.of("Austin"))
                )
            )
        );

        SearchHit hit2 = searchResponse.getHits().getHits()[2];
        assertThat(hit2.field("title").getValues(), equalTo(List.of("the third book")));
        assertThat(
            hit2.field("author").getValues(),
            equalTo(
                List.of(
                    Map.of(
                        "_id",
                        List.of("mark"),
                        "_failure",
                        List.of(
                            Map.of(
                                "reason",
                                "id [mark] on index [authors] doesn't exist",
                                "type",
                                "resource_not_found_exception",
                                "status",
                                "NOT_FOUND"
                            )
                        )
                    ),
                    Map.of("_id", List.of("mike"), "first_name", List.of("Mike"), "last_name", List.of("Boston"))
                )
            )
        );
    }

    public void testLookupMultipleIndices() {
        SearchResponse searchResponse = client().prepareSearch("books")
            .setRuntimeMappings(Map.of("publisher", Map.of("type", "lookup", "lookup_index", "publishers", "id_field", "publisher_id")))
            .setFetchSource(false)
            .addFetchField("title")
            .addFetchField("author")
            .addFetchField("publisher")
            .addSort("published_date", SortOrder.DESC)
            .setSize(2)
            .get();
        SearchHit hit0 = searchResponse.getHits().getHits()[0];
        assertThat(hit0.field("title").getValues(), equalTo(List.of("the fifth book")));
        assertThat(
            hit0.field("author").getValues(),
            equalTo(List.of(Map.of("_id", List.of("mike"), "first_name", List.of("Mike"), "last_name", List.of("Boston"))))
        );
        assertThat(
            hit0.field("publisher").getValues(),
            equalTo(List.of(Map.of("_id", List.of("p2"), "name", List.of("The second publisher"), "city", List.of("Toronto"))))
        );

        SearchHit hit1 = searchResponse.getHits().getHits()[1];
        assertThat(hit1.field("title").getValues(), equalTo(List.of("the forth book")));
        assertThat(
            hit1.field("author").getValues(),
            equalTo(
                List.of(
                    Map.of("_id", List.of("mike"), "first_name", List.of("Mike"), "last_name", List.of("Boston")),
                    Map.of("_id", List.of("jack"), "first_name", List.of("Jack"), "last_name", List.of("Austin"))
                )
            )
        );
        assertThat(
            hit1.field("publisher").getValues(),
            equalTo(List.of(Map.of("_id", List.of("p1"), "name", List.of("The first publisher"), "city", List.of("Montreal", "Vancouver"))))
        );
    }

    public void testSourceFiltering() {
        SearchResponse searchResponse = client().prepareSearch("books")
            .setFetchSource(false)
            .setRuntimeMappings(
                Map.of(
                    "full_author",
                    Map.of("type", "lookup", "lookup_index", "authors", "id_field", "author_id", "lookup_fields", List.of("*")),
                    "compact_author",
                    Map.of("type", "lookup", "lookup_index", "authors", "id_field", "author_id", "lookup_fields", List.of("first*"))
                )
            )
            .addFetchField("title")
            .addFetchField("author")
            .addFetchField("full_author")
            .addFetchField("compact_author")
            .addSort("published_date", SortOrder.DESC)
            .setSize(1)
            .get();
        SearchHit hit0 = searchResponse.getHits().getHits()[0];
        assertThat(hit0.field("title").getValues(), equalTo(List.of("the fifth book")));
        assertThat(
            hit0.field("author").getValues(),
            equalTo(List.of(Map.of("_id", List.of("mike"), "first_name", List.of("Mike"), "last_name", List.of("Boston"))))
        );
        assertThat(
            hit0.field("full_author").getValues(),
            equalTo(List.of(Map.of("_id", List.of("mike"), "first_name", List.of("Mike"), "last_name", List.of("Boston"))))
        );
        assertThat(
            hit0.field("compact_author").getValues(),
            equalTo(List.of(Map.of("_id", List.of("mike"), "first_name", List.of("Mike"))))
        );
    }

    public void testIndexNotExist() {
        SearchResponse searchResponse = client().prepareSearch("books")
            .setRuntimeMappings(Map.of("another_author", Map.of("type", "lookup", "lookup_index", "book_author", "id_field", "author_id")))
            .setFetchSource(false)
            .setQuery(new TermQueryBuilder("title", "second"))
            .addFetchField("title")
            .addFetchField("another_author")
            .get();
        ElasticsearchAssertions.assertHitCount(searchResponse, 1);
        SearchHit hit0 = searchResponse.getHits().getHits()[0];
        assertThat(hit0.field("title").getValues(), equalTo(List.of("the second book")));
        assertThat(
            hit0.field("another_author").getValues(),
            equalTo(
                List.of(
                    Map.of(
                        "_id",
                        List.of("mike"),
                        "_failure",
                        List.of(Map.of("type", "index_not_found_exception", "reason", "no such index [book_author]", "status", "NOT_FOUND"))
                    )
                )
            )
        );
    }

    public void testMatchKeyDoesNotExist() {
        SearchResponse searchResponse = client().prepareSearch("books")
            .setFetchSource(false)
            .setQuery(new TermQueryBuilder("title", "third"))
            .addFetchField("title")
            .addFetchField("author")
            .get();
        ElasticsearchAssertions.assertHitCount(searchResponse, 1);
        SearchHit hit0 = searchResponse.getHits().getHits()[0];
        assertThat(hit0.field("title").getValues(), equalTo(List.of("the third book")));
        assertThat(
            hit0.field("author").getValues(),
            equalTo(
                List.of(
                    Map.of(
                        "_id",
                        List.of("mark"),
                        "_failure",
                        List.of(
                            Map.of(
                                "type",
                                "resource_not_found_exception",
                                "reason",
                                "id [mark] on index [authors] doesn't exist",
                                "status",
                                "NOT_FOUND"
                            )
                        )
                    ),
                    Map.of("_id", List.of("mike"), "first_name", List.of("Mike"), "last_name", List.of("Boston"))
                )
            )
        );
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
            .addFetchField("title")
            .addFetchField("author")
            .addSort("published_date", SortOrder.DESC)
            .setSize(2)
            .get();
        assertThat(searchResponse.getHits().getHits(), arrayWithSize(2));
        SearchHit hit0 = searchResponse.getHits().getHits()[0];
        assertThat(hit0.field("title").getValues(), equalTo(List.of("the fifth book")));
        assertThat(
            hit0.field("author").getValues(),
            equalTo(List.of(Map.of("_id", List.of("mike"), "first_name", List.of("Mike"), "last_name", List.of("Boston"))))
        );

        SearchHit hit1 = searchResponse.getHits().getHits()[1];
        assertThat(hit1.field("title").getValues(), equalTo(List.of("the forth book")));
        assertThat(
            hit1.field("author").getValues(),
            equalTo(
                List.of(
                    Map.of("_id", List.of("mike"), "first_name", List.of("Mike"), "last_name", List.of("Boston")),
                    Map.of("_id", List.of("jack"), "first_name", List.of("Jack"), "last_name", List.of("Austin"))
                )
            )
        );
        assertThat(lookupRequests.get(), greaterThan(0));
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            MockTransportService mockTransportService = (MockTransportService) transportService;
            mockTransportService.clearAllRules();
        }
    }
}
