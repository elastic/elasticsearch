/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.fetch.subphase.highlight;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHighlight;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration test for highlighters registered by a plugin.
 */
@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class CustomHighlighterSearchIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CustomHighlighterPlugin.class);
    }

    @Before
    protected void setup() throws Exception{
        indexRandom(true,
                client().prepareIndex("test").setId("1").setSource(
                        "name", "arbitrary content", "other_name", "foo", "other_other_name", "bar"),
                client().prepareIndex("test").setId("2").setSource(
                        "other_name", "foo", "other_other_name", "bar"));
    }

    public void testThatCustomHighlightersAreSupported() throws IOException {
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchAllQuery())
                .highlighter(new HighlightBuilder().field("name").highlighterType("test-custom"))
                .get();
        assertHighlight(searchResponse, 0, "name", 0, equalTo("standard response for name at position 1"));
    }

    public void testThatCustomHighlighterCanBeConfiguredPerField() throws Exception {
        HighlightBuilder.Field highlightConfig = new HighlightBuilder.Field("name");
        highlightConfig.highlighterType("test-custom");
        Map<String, Object> options = new HashMap<>();
        options.put("myFieldOption", "someValue");
        highlightConfig.options(options);

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.matchAllQuery())
                .highlighter(new HighlightBuilder().field(highlightConfig))
                .get();

        assertHighlight(searchResponse, 0, "name", 0, equalTo("standard response for name at position 1"));
        assertHighlight(searchResponse, 0, "name", 1, equalTo("field:myFieldOption:someValue"));
    }

    public void testThatCustomHighlighterCanBeConfiguredGlobally() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("myGlobalOption", "someValue");

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .highlighter(new HighlightBuilder().field("name").highlighterType("test-custom").options(options))
                .get();

        assertHighlight(searchResponse, 0, "name", 0, equalTo("standard response for name at position 1"));
        assertHighlight(searchResponse, 0, "name", 1, equalTo("field:myGlobalOption:someValue"));
    }

    public void testThatCustomHighlighterReceivesFieldsInOrder() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()).should(QueryBuilders
                        .termQuery("name", "arbitrary")))
                .highlighter(
                        new HighlightBuilder().highlighterType("test-custom").field("name").field("other_name").field("other_other_name")
                                .useExplicitFieldOrder(true))
                .get();

        assertHighlight(searchResponse, 0, "name", 0, equalTo("standard response for name at position 1"));
        assertHighlight(searchResponse, 0, "other_name", 0, equalTo("standard response for other_name at position 2"));
        assertHighlight(searchResponse, 0, "other_other_name", 0, equalTo("standard response for other_other_name at position 3"));
        assertHighlight(searchResponse, 1, "name", 0, equalTo("standard response for name at position 1"));
        assertHighlight(searchResponse, 1, "other_name", 0, equalTo("standard response for other_name at position 2"));
        assertHighlight(searchResponse, 1, "other_other_name", 0, equalTo("standard response for other_other_name at position 3"));
    }
}
