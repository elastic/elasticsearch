/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class LookupRuntimeFieldIT extends ESIntegTestCase {

    @Before
    public void populateIndex() throws Exception {
        indicesAdmin().prepareCreate("authors")
            .setMapping("author", "type=keyword", "joined", "type=date,format=yyyy-MM-dd")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .get();
        List<Map<String, String>> authors = List.of(
            Map.of("author", "john", "first_name", "John", "last_name", "New York", "joined", "2020-03-01"),
            Map.of("author", "mike", "first_name", "Mike", "last_name", "Boston", "joined", "2010-06-20"),
            Map.of("author", "jack", "first_name", "Jack", "last_name", "Austin", "joined", "1999-11-03")
        );
        for (Map<String, String> author : authors) {
            client().prepareIndex("authors").setSource(author).setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values())).get();
        }
        client().admin().indices().prepareRefresh("authors").get();

        indicesAdmin().prepareCreate("publishers")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5)))
            .get();
        client().prepareBulk("publishers")
            .add(new IndexRequest().id("p1").source("name", "The first publisher", "city", List.of("Montreal", "Vancouver")))
            .add(new IndexRequest().id("p2").source("name", "The second publisher", "city", "Toronto"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        indicesAdmin().prepareCreate("books").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)).setMapping("""
            {
                "properties": {
                    "title": {"type": "text"},
                    "author_id": {"type": "keyword"},
                    "genre": {"type": "keyword"},
                    "published_date": {
                        "type": "date",
                        "format": "yyyy-MM-dd"
                    }
                },
                "runtime": {
                    "author": {
                        "type": "lookup",
                        "target_index": "authors",
                        "input_field": "author_id",
                        "target_field": "author",
                        "fetch_fields": ["first_name", "last_name"]
                    }
                }
            }
            """).get();
        List<Map<String, Object>> books = List.of(
            Map.of(
                "title",
                "the first book",
                "genre",
                "fiction",
                "author_id",
                "john",
                "publisher_id",
                "p1",
                "published_date",
                "2020-01-05"
            ),
            Map.of(
                "title",
                "the second book",
                "genre",
                "science",
                "author_id",
                "mike",
                "publisher_id",
                "p2",
                "published_date",
                "2020-02-10"
            ),
            Map.of(
                "title",
                "the third book",
                "genre",
                "science",
                "author_id",
                List.of("mark", "mike"),
                "publisher_id",
                "p1",
                "published_date",
                "2021-04-20"
            ),
            Map.of(
                "title",
                "the forth book",
                "genre",
                "fiction",
                "author_id",
                List.of("mike", "jack"),
                "publisher_id",
                "p1",
                "published_date",
                "2021-05-11"
            ),
            Map.of("title", "the fifth book", "genre", "science", "author_id", "mike", "publisher_id", "p2", "published_date", "2021-06-30")
        );
        for (Map<String, Object> book : books) {
            client().prepareIndex("books").setSource(book).setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values())).get();
        }
        indicesAdmin().prepareRefresh("books").get();
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
                        "target_index": "publishers",
                        "input_field": "publisher_id",
                        "target_field": "_id",
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
                    "target_index": "authors",
                    "input_field": "author_id",
                    "target_field": "author",
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

    private Map<String, Object> parseMapping(String mapping) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, mapping)) {
            return parser.map();
        }
    }
}
