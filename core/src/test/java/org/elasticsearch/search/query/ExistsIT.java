/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.query;

import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.internal.FieldNamesFieldMapper;
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
        ensureYellow("test");
        SearchResponse resp = client().prepareSearch("test").setQuery(QueryBuilders.existsQuery("foo")).execute().actionGet();
        assertSearchResponse(resp);
        resp = client().prepareSearch("test").setQuery(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery("foo"))).execute().actionGet();
        assertSearchResponse(resp);
    }

    public void testExists() throws Exception {
        XContentBuilder mapping = XContentBuilder.builder(JsonXContent.jsonXContent)
            .startObject()
                .startObject("type")
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

        assertAcked(client().admin().indices().prepareCreate("idx").addMapping("type", mapping));
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
        List<IndexRequestBuilder> reqs = new ArrayList<IndexRequestBuilder>();
        for (Map<String, Object> source : sources) {
            reqs.add(client().prepareIndex("idx", "type").setSource(source));
        }
        // We do NOT index dummy documents, otherwise the type for these dummy documents
        // would have _field_names indexed while the current type might not which might
        // confuse the exists/missing parser at query time
        indexRandom(true, false, reqs);

        final Map<String, Integer> expected = new LinkedHashMap<String, Integer>();
        expected.put("foo", 1);
        expected.put("f*", 1);
        expected.put("bar", 2);
        expected.put("bar.*", 2);
        expected.put("bar.foo", 1);
        expected.put("bar.bar", 1);
        expected.put("bar.bar.bar", 1);
        expected.put("foobar", 0);

        ensureYellow("idx");
        final long numDocs = sources.length;
        SearchResponse allDocs = client().prepareSearch("idx").setSize(sources.length).get();
        assertSearchResponse(allDocs);
        assertHitCount(allDocs, numDocs);
        for (Map.Entry<String, Integer> entry : expected.entrySet()) {
            final String fieldName = entry.getKey();
            final int count = entry.getValue();
            // exists
            SearchResponse resp = client().prepareSearch("idx").setQuery(QueryBuilders.existsQuery(fieldName)).execute().actionGet();
            assertSearchResponse(resp);
            try {
                assertEquals(String.format(Locale.ROOT, "exists(%s, %d) mapping: %s response: %s", fieldName, count, mapping.string(), resp), count, resp.getHits().totalHits());
            } catch (AssertionError e) {
                for (SearchHit searchHit : allDocs.getHits()) {
                    final String index = searchHit.getIndex();
                    final String type = searchHit.getType();
                    final String id = searchHit.getId();
                    final ExplainResponse explanation = client().prepareExplain(index, type, id).setQuery(QueryBuilders.existsQuery(fieldName)).get();
                    logger.info("Explanation for [{}] / [{}] / [{}]: [{}]", fieldName, id, searchHit.getSourceAsString(), explanation.getExplanation());
                }
                throw e;
            }
        }
    }
}
