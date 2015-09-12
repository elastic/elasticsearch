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

import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.groovy.GroovyScriptEngineService;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertExists;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSuggestion;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * Tests for transforming the source document before indexing.
 */
@SuppressCodecs("*") // requires custom completion format
public class TransformOnIndexMapperIT extends ESIntegTestCase {
    @Test
    public void searchOnTransformed() throws Exception {
        setup(true);

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

    // TODO: the completion suggester currently returns payloads with no reencoding so this test
    // exists to make sure that _source transformation and completion work well together. If we
    // ever fix the completion suggester to reencode the payloads then we can remove this test.
    @Test
    public void contextSuggestPayloadTransformed() throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder.startObject("properties");
        builder.startObject("suggest").field("type", "completion").field("payloads", true).endObject();
        builder.endObject();
        builder.startObject("transform");
        builder.field("script", "ctx._source.suggest = ['input': ctx._source.text];ctx._source.suggest.payload = ['display': ctx._source.text, 'display_detail': 'on the fly']");
        builder.field("lang", GroovyScriptEngineService.NAME);
        builder.endObject();
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("test", builder));
        // Payload is stored using original source format (json, smile, yaml, whatever)
        XContentType type = XContentType.values()[between(0, XContentType.values().length - 1)];
        XContentBuilder source = XContentFactory.contentBuilder(type);
        source.startObject().field("text", "findme").endObject();
        indexRandom(true, client().prepareIndex("test", "test", "findme").setSource(source));
        SuggestResponse response = client().prepareSuggest("test").addSuggestion(
                SuggestBuilders.completionSuggestion("test").field("suggest").text("findme")).get();
        assertSuggestion(response.getSuggest(), 0, 0, "test", "findme");
        CompletionSuggestion.Entry.Option option = (CompletionSuggestion.Entry.Option)response.getSuggest().getSuggestion("test").getEntries().get(0).getOptions().get(0);
        // And it comes back in exactly that way.
        XContentBuilder expected = XContentFactory.contentBuilder(type);
        expected.startObject().field("display", "findme").field("display_detail", "on the fly").endObject();
        assertEquals(expected.string(), option.getPayloadAsString());
    }

    /**
     * Setup an index with some source transforms. Randomly picks the number of
     * transforms but all but one of the transforms is a noop. The other is a
     * script that fills the 'destination' field with the 'content' field only
     * if the 'title' field starts with 't' and then always removes the
     * 'content' field regarless of the contents of 't'. The actual script
     * randomly uses parameters or not.
     *
     * @param forceRefresh
     *            should the data be flushed to disk? Set to false to test real
     *            time fetching
     */
    private void setup(boolean forceRefresh) throws IOException, InterruptedException, ExecutionException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder.field("transform");
        if (getRandom().nextBoolean()) {
            // Single transform
            builder.startObject();
            buildTransformScript(builder);
            builder.field("lang", randomFrom(null, GroovyScriptEngineService.NAME));
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
                builder.field("lang", randomFrom(null, GroovyScriptEngineService.NAME));
                builder.endObject();
            }
            builder.endArray();
        }
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("test", builder));

        indexRandom(forceRefresh, client().prepareIndex("test", "test", "notitle").setSource("content", "findme"),
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
