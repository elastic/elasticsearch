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
package org.elasticsearch.search.highlight;

import com.google.common.collect.Maps;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHighlight;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public class CustomHighlighterSearchTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder()
                .put("plugin.types", CustomHighlighterPlugin.class.getName())
                .put(super.nodeSettings(nodeOrdinal))
                .build();
    }

    @Before
    protected void setup() throws Exception{
        client().prepareIndex("test", "test", "1").setSource(XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "arbitrary content")
                .endObject())
                .setRefresh(true).execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();
    }

    @Test
    public void testThatCustomHighlightersAreSupported() throws IOException {
        SearchResponse searchResponse = client().prepareSearch("test").setTypes("test")
                .setQuery(QueryBuilders.matchAllQuery())
                .addHighlightedField("name").setHighlighterType("test-custom")
                .execute().actionGet();
        assertHighlight(searchResponse, 0, "name", 0, equalTo("standard response"));
    }

    @Test
    public void testThatCustomHighlighterCanBeConfiguredPerField() throws Exception {
        HighlightBuilder.Field highlightConfig = new HighlightBuilder.Field("name");
        highlightConfig.highlighterType("test-custom");
        Map<String, Object> options = Maps.newHashMap();
        options.put("myFieldOption", "someValue");
        highlightConfig.options(options);

        SearchResponse searchResponse = client().prepareSearch("test").setTypes("test")
                .setQuery(QueryBuilders.matchAllQuery())
                .addHighlightedField(highlightConfig)
                .execute().actionGet();

        assertHighlight(searchResponse, 0, "name", 0, equalTo("standard response"));
        assertHighlight(searchResponse, 0, "name", 1, equalTo("field:myFieldOption:someValue"));
    }

    @Test
    public void testThatCustomHighlighterCanBeConfiguredGlobally() throws Exception {
        Map<String, Object> options = Maps.newHashMap();
        options.put("myGlobalOption", "someValue");

        SearchResponse searchResponse = client().prepareSearch("test").setTypes("test")
                .setQuery(QueryBuilders.matchAllQuery())
                .setHighlighterOptions(options)
                .setHighlighterType("test-custom")
                .addHighlightedField("name")
                .execute().actionGet();

        assertHighlight(searchResponse, 0, "name", 0, equalTo("standard response"));
        assertHighlight(searchResponse, 0, "name", 1, equalTo("field:myGlobalOption:someValue"));
    }
}
