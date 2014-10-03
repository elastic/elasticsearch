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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.internal.FieldNamesFieldMapper;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import java.util.*;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;


public class ExistsMissingTests extends ElasticsearchIntegrationTest {

    public void testExistsMissing() throws Exception {

        XContentBuilder mapping = XContentBuilder.builder(JsonXContent.jsonXContent)
            .startObject()
                .startObject("type")
                    .startObject(FieldNamesFieldMapper.NAME)
                        // by setting randomly index to no we also test the pre-1.3 behavior
                        .field("index", randomFrom("no", "not_analyzed"))
                        .field("store", randomFrom("no", "yes"))
                    .endObject()
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "string")
                        .endObject()
                        .startObject("bar")
                            .field("type", "object")
                            .startObject("properties")
                                .startObject("foo")
                                    .field("type", "string")
                                .endObject()
                                .startObject("bar")
                                    .field("type", "object")
                                    .startObject("properties")
                                        .startObject("bar")
                                            .field("type", "string")
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
        @SuppressWarnings("unchecked")
        final Map<String, Object>[] sources = new Map[] {
                // simple property
                ImmutableMap.of("foo", "bar"),
                // object fields
                ImmutableMap.of("bar", ImmutableMap.of("foo", "bar", "bar", ImmutableMap.of("bar", "foo"))),
                ImmutableMap.of("bar", ImmutableMap.of("baz", 42)),
                // empty doc
                ImmutableMap.of()
        };
        List<IndexRequestBuilder> reqs = new ArrayList<IndexRequestBuilder>();
        for (Map<String, Object> source : sources) {
            reqs.add(client().prepareIndex("idx", "type").setSource(source));
        }
        indexRandom(true, reqs);

        final Map<String, Integer> expected = new LinkedHashMap<String, Integer>();
        expected.put("foo", 1);
        expected.put("f*", 2); // foo and bar.foo, that's how the expansion works
        expected.put("bar", 2);
        expected.put("bar.*", 2);
        expected.put("bar.foo", 1);
        expected.put("bar.bar", 1);
        expected.put("bar.bar.bar", 1);
        expected.put("baz", 1);
        expected.put("foobar", 0);

        ensureYellow("idx");
        final long numDocs = sources.length;
        SearchResponse resp = client().prepareSearch("idx").get();
        assertSearchResponse(resp);
        assertHitCount(resp, numDocs);
        for (Map.Entry<String, Integer> entry : expected.entrySet()) {
            final String fieldName = entry.getKey();
            final int count = entry.getValue();
            // exists
            resp = client().prepareSearch("idx").setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders.existsFilter(fieldName))).execute().actionGet();
            assertSearchResponse(resp);
            assertEquals(String.format(Locale.ROOT, "exists(%s, %d) mapping: %s response: %s", fieldName, count, mapping.string(), resp), count, resp.getHits().totalHits());

            // missing
            resp = client().prepareSearch("idx").setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders.missingFilter(fieldName))).execute().actionGet();
            assertSearchResponse(resp);
            assertEquals(String.format(Locale.ROOT, "missing(%s, %d) mapping: %s response: %s", fieldName, count, mapping.string(), resp), numDocs - count, resp.getHits().totalHits());
        }
    }

}
