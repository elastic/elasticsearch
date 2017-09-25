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

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

public class AllFieldIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class); // uses index.version.created
    }

    public void test5xIndicesContinueToUseAll() throws Exception {
        // Default 5.x settings
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put("index.version.created", Version.V_5_1_1.id)));
        client().prepareIndex("test", "type", "1").setSource("body", "foo").get();
        refresh();
        SearchResponse resp = client().prepareSearch("test").setQuery(QueryBuilders.matchQuery("_all", "foo")).get();
        assertHitCount(resp, 1);
        assertSearchHits(resp, "1");

        // _all explicitly enabled
        assertAcked(prepareCreate("test2")
                .setSource(jsonBuilder()
                        .startObject()
                        .startObject("mappings")
                        .startObject("type")
                        .startObject("_all")
                        .field("enabled", true)
                        .endObject() // _all
                        .endObject() // type
                        .endObject() // mappings
                        .endObject())
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_4_0_ID)));
        client().prepareIndex("test2", "type", "1").setSource("foo", "bar").get();
        refresh();
        resp = client().prepareSearch("test2").setQuery(QueryBuilders.matchQuery("_all", "bar")).get();
        assertHitCount(resp, 1);
        assertSearchHits(resp, "1");

        // _all explicitly disabled
        assertAcked(prepareCreate("test3")
                .setSource(jsonBuilder()
                        .startObject()
                        .startObject("mappings")
                        .startObject("type")
                        .startObject("_all")
                        .field("enabled", false)
                        .endObject() // _all
                        .endObject() // type
                        .endObject() // mappings
                        .endObject())
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_4_0_ID)));
        client().prepareIndex("test3", "type", "1").setSource("foo", "baz").get();
        refresh();
        resp = client().prepareSearch("test3").setQuery(QueryBuilders.matchQuery("_all", "baz")).get();
        assertHitCount(resp, 0);

        // _all present, but not enabled or disabled (default settings)
        assertAcked(prepareCreate("test4")
                .setSource(jsonBuilder()
                        .startObject()
                        .startObject("mappings")
                        .startObject("type")
                        .startObject("_all")
                        .endObject() // _all
                        .endObject() // type
                        .endObject() // mappings
                        .endObject())
                .setSettings(Settings.builder().put("index.version.created", Version.V_5_4_0_ID)));
        client().prepareIndex("test4", "type", "1").setSource("foo", "eggplant").get();
        refresh();
        resp = client().prepareSearch("test4").setQuery(QueryBuilders.matchQuery("_all", "eggplant")).get();
        assertHitCount(resp, 1);
        assertSearchHits(resp, "1");
    }

}
