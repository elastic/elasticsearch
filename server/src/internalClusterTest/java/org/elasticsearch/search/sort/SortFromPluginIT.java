/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.plugin.CustomSortBuilder;
import org.elasticsearch.search.sort.plugin.CustomSortPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.Matchers.equalTo;

public class SortFromPluginIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CustomSortPlugin.class, InternalSettingsPlugin.class);
    }

    public void testPluginSort() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("field", 2).get();
        client().prepareIndex("test").setId("2").setSource("field", 1).get();
        client().prepareIndex("test").setId("3").setSource("field", 0).get();

        refresh();

        SearchResponse searchResponse = client().prepareSearch("test").addSort(new CustomSortBuilder("field", SortOrder.ASC)).get();
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("1"));

        searchResponse = client().prepareSearch("test").addSort(new CustomSortBuilder("field", SortOrder.DESC)).get();
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
    }

    public void testPluginSortXContent() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("field", 2).get();
        client().prepareIndex("test").setId("2").setSource("field", 1).get();
        client().prepareIndex("test").setId("3").setSource("field", 0).get();

        refresh();

        // builder -> json -> builder
        SearchResponse searchResponse = client().prepareSearch("test")
            .setSource(
                SearchSourceBuilder.fromXContent(
                    createParser(
                        JsonXContent.jsonXContent,
                        new SearchSourceBuilder().sort(new CustomSortBuilder("field", SortOrder.ASC)).toString()
                    )
                )
            )
            .get();

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("1"));

        searchResponse = client().prepareSearch("test")
            .setSource(
                SearchSourceBuilder.fromXContent(
                    createParser(
                        JsonXContent.jsonXContent,
                        new SearchSourceBuilder().sort(new CustomSortBuilder("field", SortOrder.DESC)).toString()
                    )
                )
            )
            .get();

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
    }
}
