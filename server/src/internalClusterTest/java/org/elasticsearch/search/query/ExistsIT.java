/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;

public class ExistsIT extends ESIntegTestCase {

    // TODO: move this to a unit test somewhere...
    public void testEmptyIndex() throws Exception {
        createIndex("test");
        SearchResponse resp = client().prepareSearch("test").setQuery(QueryBuilders.existsQuery("foo")).get();
        assertSearchResponse(resp);
        resp = client().prepareSearch("test").setQuery(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("foo")))
                .get();
        assertSearchResponse(resp);
    }

    public void testExists() throws Exception {
        XContentBuilder mapping = XContentBuilder.builder(JsonXContent.jsonXContent)
            .startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "text")
                        .endObject()
                        .startObject("bar")
                            .field("type", "object")
                            .startObject("properties")
                                .startObject("foo")
                                    .field("type", "text")
                                .endObject()
                                .startObject("bar")
                                    .field("type", "object")
                                    .startObject("properties")
                                        .startObject("bar")
                                            .field("type", "text")
                                        .endObject()
                                    .endObject()
                                .endObject()
                                .startObject("baz")
                                    .field("type", "long")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();

        assertAcked(client().admin().indices().prepareCreate("idx").setMapping(mapping));
        Map<String, Object> barObject = new HashMap<>();
        barObject.put("foo", "bar");
        barObject.put("bar", singletonMap("bar", "foo"));
        @SuppressWarnings("unchecked")
        final Map<String, Object>[] sources = new Map[] {
                // simple property
                singletonMap("foo", "bar"),
                // object fields
                singletonMap("bar", barObject),
                singletonMap("bar", singletonMap("baz", 42)),
                // empty doc
                emptyMap()
        };
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (Map<String, Object> source : sources) {
            reqs.add(client().prepareIndex("idx").setSource(source));
        }
        // We do NOT index dummy documents, otherwise the type for these dummy documents
        // would have _field_names indexed while the current type might not which might
        // confuse the exists/missing parser at query time
        indexRandom(true, false, reqs);

        final Map<String, Integer> expected = new LinkedHashMap<>();
        expected.put("foo", 1);
        expected.put("f*", 1);
        expected.put("bar", 2);
        expected.put("bar.*", 2);
        expected.put("bar.foo", 1);
        expected.put("bar.bar", 1);
        expected.put("bar.bar.bar", 1);
        expected.put("foobar", 0);

        final long numDocs = sources.length;
        SearchResponse allDocs = client().prepareSearch("idx").setSize(sources.length).get();
        assertSearchResponse(allDocs);
        assertHitCount(allDocs, numDocs);
        for (Map.Entry<String, Integer> entry : expected.entrySet()) {
            final String fieldName = entry.getKey();
            final int count = entry.getValue();
            // exists
            SearchResponse resp = client().prepareSearch("idx").setQuery(QueryBuilders.existsQuery(fieldName)).get();
            assertSearchResponse(resp);
            try {
                assertEquals(String.format(Locale.ROOT, "exists(%s, %d) mapping: %s response: %s", fieldName, count,
                        Strings.toString(mapping), resp), count, resp.getHits().getTotalHits().value);
            } catch (AssertionError e) {
                for (SearchHit searchHit : allDocs.getHits()) {
                    final String index = searchHit.getIndex();
                    final String id = searchHit.getId();
                    final ExplainResponse explanation = client().prepareExplain(index, id)
                            .setQuery(QueryBuilders.existsQuery(fieldName)).get();
                    logger.info("Explanation for [{}] / [{}] / [{}]: [{}]", fieldName, id, searchHit.getSourceAsString(),
                            explanation.getExplanation());
                }
                throw e;
            }
        }
    }

    public void testFieldAlias() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("bar")
                            .field("type", "long")
                        .endObject()
                        .startObject("foo")
                            .field("type", "object")
                            .startObject("properties")
                                .startObject("bar")
                                    .field("type", "double")
                                .endObject()
                            .endObject()
                        .endObject()
                        .startObject("foo-bar")
                            .field("type", "alias")
                            .field("path", "foo.bar")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        assertAcked(prepareCreate("idx").setMapping(mapping));
        ensureGreen("idx");

        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        indexRequests.add(client().prepareIndex("idx").setSource(emptyMap()));
        indexRequests.add(client().prepareIndex("idx").setSource(emptyMap()));
        indexRequests.add(client().prepareIndex("idx").setSource("bar", 3));
        indexRequests.add(client().prepareIndex("idx").setSource("foo", singletonMap("bar", 2.718)));
        indexRequests.add(client().prepareIndex("idx").setSource("foo", singletonMap("bar", 6.283)));
        indexRandom(true, false, indexRequests);

        Map<String, Integer> expected = new LinkedHashMap<>();
        expected.put("foo.bar", 2);
        expected.put("foo-bar", 2);
        expected.put("foo*", 2);
        expected.put("*bar", 3);

        for (Map.Entry<String, Integer> entry : expected.entrySet()) {
            String fieldName = entry.getKey();
            int expectedCount = entry.getValue();

            SearchResponse response = client().prepareSearch("idx")
                .setQuery(QueryBuilders.existsQuery(fieldName))
                .get();
            assertSearchResponse(response);
            assertHitCount(response, expectedCount);
        }
    }

    public void testFieldAliasWithNoDocValues() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "long")
                            .field("doc_values", false)
                        .endObject()
                        .startObject("foo-alias")
                            .field("type", "alias")
                            .field("path", "foo")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
        assertAcked(prepareCreate("idx").setMapping(mapping));
        ensureGreen("idx");

        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        indexRequests.add(client().prepareIndex("idx").setSource(emptyMap()));
        indexRequests.add(client().prepareIndex("idx").setSource(emptyMap()));
        indexRequests.add(client().prepareIndex("idx").setSource("foo", 3));
        indexRequests.add(client().prepareIndex("idx").setSource("foo", 43));
        indexRandom(true, false, indexRequests);

        SearchResponse response = client().prepareSearch("idx")
            .setQuery(QueryBuilders.existsQuery("foo-alias"))
            .get();
        assertSearchResponse(response);
        assertHitCount(response, 2);
    }
}
