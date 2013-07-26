/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.test.integration.search.highlight;

import com.google.common.collect.Maps;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHighlight;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class CustomHighlighterSearchTests extends AbstractNodesTests {

    @Override
    protected void beforeClass() throws Exception{
        ImmutableSettings.Builder settings = settingsBuilder().put("plugin.types", CustomHighlighterPlugin.class.getName());
        startNode("server1", settings);
        Client client = client();

        client.prepareIndex("test", "test", "1").setSource(XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "arbitrary content")
                .endObject())
                .setRefresh(true).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForYellowStatus().execute().actionGet();
    }

    @Override
    public Client client() {
        return client("server1");
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
