/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

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
            .setMapping("author", "type=keyword", "joined", "type=date,format=yyyy-MM-dd")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .get();
        client().prepareBulk("authors")
            .add(new IndexRequest().source("author", "john", "first_name", "John", "last_name", "New York", "joined", "2020-03-01"))
            .add(new IndexRequest().source("author", "mike", "first_name", "Mike", "last_name", "Boston", "joined", "2010-06-20"))
            .add(new IndexRequest().source("author", "jack", "first_name", "Jack", "last_name", "Austin", "joined", "1999-11-03"))
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
            .setMapping("""
                {
                    "properties": {
                        "title": {"type": "text"},
                        "author_id": {"type": "keyword"},
                        "published_date": {
                            "type": "date",
                            "format": "yyyy-MM-dd"
                        }
                    },
                    "runtime": {
                        "author": {
                            "type": "lookup",
                            "index": "authors",
                            "query_type": "term",
                            "query_input_field": "author_id",
                            "query_target_field": "author",
                            "fetch_fields": ["first_name", "last_name"]
                        }
                    }
                }
                """)
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
            equalTo(List.of(Map.of("first_name", List.of("Mike"), "last_name", List.of("Boston"))))
        );

        SearchHit hit1 = searchResponse.getHits().getHits()[1];
        assertThat(hit1.field("title").getValues(), equalTo(List.of("the forth book")));
        assertThat(
            hit1.field("author").getValues(),
            equalTo(
                List.of(
                    Map.of("first_name", List.of("Mike"), "last_name", List.of("Boston")),
                    Map.of("first_name", List.of("Jack"), "last_name", List.of("Austin"))
                )
            )
        );

        SearchHit hit2 = searchResponse.getHits().getHits()[2];
        assertThat(hit2.field("title").getValues(), equalTo(List.of("the third book")));
        assertThat(
            hit2.field("author").getValues(),
            equalTo(List.of(Map.of("first_name", List.of("Mike"), "last_name", List.of("Boston"))))
        );
    }

    public void testLookupMultipleIndices() throws IOException {
        SearchResponse searchResponse = client().prepareSearch("books")
            .setRuntimeMappings(parseMapping("""
                {
                    "publisher": {
                        "type": "lookup",
                        "index": "publishers",
                        "query_type": "term",
                        "query_input_field": "publisher_id",
                        "query_target_field": "_id",
                        "fetch_fields": ["name", "city"]
                    }
                }
                """))
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
            equalTo(List.of(Map.of("first_name", List.of("Mike"), "last_name", List.of("Boston"))))
        );
        assertThat(
            hit0.field("publisher").getValues(),
            equalTo(List.of(Map.of("name", List.of("The second publisher"), "city", List.of("Toronto"))))
        );

        SearchHit hit1 = searchResponse.getHits().getHits()[1];
        assertThat(hit1.field("title").getValues(), equalTo(List.of("the forth book")));
        assertThat(
            hit1.field("author").getValues(),
            equalTo(
                List.of(
                    Map.of("first_name", List.of("Mike"), "last_name", List.of("Boston")),
                    Map.of("first_name", List.of("Jack"), "last_name", List.of("Austin"))
                )
            )
        );
        assertThat(
            hit1.field("publisher").getValues(),
            equalTo(List.of(Map.of("name", List.of("The first publisher"), "city", List.of("Montreal", "Vancouver"))))
        );
    }

    public void testFetchField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("books").setRuntimeMappings(parseMapping("""
            {
                "author": {
                    "type": "lookup",
                    "index": "authors",
                    "query_type": "term",
                    "query_input_field": "author_id",
                    "query_target_field": "author",
                    "fetch_fields": ["first_name", {"field": "joined", "format": "MM/yyyy"}]
                }
            }
            """)).addFetchField("author").addFetchField("title").addSort("published_date", SortOrder.ASC).setSize(1).get();
        ElasticsearchAssertions.assertNoFailures(searchResponse);
        SearchHit hit0 = searchResponse.getHits().getHits()[0];
        // "author", "john", "first_name", "John", "last_name", "New York", "joined", "2020-03-01"
        assertThat(hit0.field("title").getValues(), equalTo(List.of("the first book")));
        assertThat(hit0.field("author").getValues(), equalTo(List.of(Map.of("first_name", List.of("John"), "joined", List.of("03/2020")))));
    }

    public void testIndexNotExist() throws IOException {
        IndexNotFoundException failure = expectThrows(
            IndexNotFoundException.class,
            () -> client().prepareSearch("books")
                .setRuntimeMappings(parseMapping("""
                    {
                        "another_author": {
                            "type": "lookup",
                            "index": "book_author",
                            "query_type": "term",
                            "query_input_field": "author_id",
                            "query_target_field": "another_field",
                            "fetch_fields": ["name*"]
                        }
                    }
                    """))
                .setFetchSource(false)
                .setQuery(new TermQueryBuilder("title", "second"))
                .addFetchField("title")
                .addFetchField("another_author")
                .get()
        );
        assertThat(failure.getMessage(), equalTo("no such index [book_author]"));
    }

    public void testExpensiveQueries() throws Exception {
        // Coordinating node doesn't allow expensive queries
        final String newNode = internalCluster().startNode(Settings.builder().put("search.allow_expensive_queries", false));
        final ElasticsearchException error = expectThrows(
            ElasticsearchException.class,
            () -> client(newNode).prepareSearch("books").addFetchField("title").addFetchField("author").get()
        );
        assertThat(
            error.getMessage(),
            equalTo("cannot be executed against lookup field while [search.allow_expensive_queries] is set to [false].")
        );
        internalCluster().stopNode(newNode);
    }

    private Map<String, Object> parseMapping(String mapping) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, mapping)) {
            return parser.map();
        }
    }
}
