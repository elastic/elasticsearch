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

package org.elasticsearch.index.mapper;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 * Tests for transforming the source document before indexing.
 */
public class TransformOnIndexMapperIntegrationTest extends ElasticsearchIntegrationTest {
    @Test
    public void searchOnTransformed() throws Exception {
        setup(false);

        // Searching by the field created in the transport finds the entry
        SearchResponse response = client().prepareSearch("test").setQuery(termQuery("destination", "findme")).get();
        assertSearchHits(response, "righttitle");
        // The field built in the transform isn't in the source but source is,
        // even though we didn't index it!
        assertRightTitleSourceUntransformed(response.getHits().getAt(0).sourceAsMap());

        // Can't find by a field removed from the document by the transform
        response = client().prepareSearch("test").setQuery(termQuery("content", "findme")).get();
        assertHitCount(response, 0);
    }

    @Test
    public void getTransformed() throws Exception {
        setup(getRandom().nextBoolean());
        GetResponse response = client().prepareGet("test", "test", "righttitle").get();
        assertExists(response);
        assertRightTitleSourceUntransformed(response.getSource());

        response = client().prepareGet("test", "test", "righttitle").setTransformSource(true).get();
        assertExists(response);
        assertRightTitleSourceTransformed(response.getSource());
    }

    /**
     * Setup an index with some source transforms. Randomly picks the number of
     * transforms but all but one of the transforms is a noop. The other is a
     * script that fills the 'destination' field with the 'content' field only
     * if the 'title' field starts with 't' and then always removes the
     * 'content' field regarless of the contents of 't'. The actual script
     * randomly uses parameters or not.
     * 
     * @param flush
     *            should the data be flushed to disk? Set to false to test real
     *            time fetching
     */
    private void setup(boolean flush) throws IOException, InterruptedException, ExecutionException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder.field("transform");
        if (getRandom().nextBoolean()) {
            // Single transform
            builder.startObject();
            buildTransformScript(builder);
            builder.field("lang", "groovy");
            builder.endObject();
        } else {
            // Multiple transforms
            int total = between(1, 10);
            int actual = between(0, total - 1);
            builder.startArray();
            for (int s = 0; s < total; s++) {
                builder.startObject();
                if (s == actual) {
                    buildTransformScript(builder);
                } else {
                    builder.field("script", "true");
                }
                builder.field("lang", "groovy");
                builder.endObject();
            }
            builder.endArray();
        }
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("test", builder));

        indexRandom(!flush, client().prepareIndex("test", "test", "notitle").setSource("content", "findme"),
                client().prepareIndex("test", "test", "badtitle").setSource("content", "findme", "title", "cat"),
                client().prepareIndex("test", "test", "righttitle").setSource("content", "findme", "title", "table"));
    }

    private void buildTransformScript(XContentBuilder builder) throws IOException {
        String script = "if (ctx._source['title']?.startsWith('t')) { ctx._source['destination'] = ctx._source[sourceField] }; ctx._source.remove(sourceField);";
        if (getRandom().nextBoolean()) {
            script = script.replace("sourceField", "'content'");
        } else {
            builder.field("params", ImmutableMap.of("sourceField", "content"));
        }
        builder.field("script", script);
    }

    private void assertRightTitleSourceUntransformed(Map<String, Object> source) {
        assertThat(source, both(hasEntry("content", (Object) "findme")).and(not(hasKey("destination"))));
    }

    private void assertRightTitleSourceTransformed(Map<String, Object> source) {
        assertThat(source, both(hasEntry("destination", (Object) "findme")).and(not(hasKey("content"))));
    }

}
