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

package org.elasticsearch.index;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class HiddenIndexIT extends ESIntegTestCase {

    public void testHiddenIndexSearch() {
        assertAcked(client().admin().indices().prepareCreate("hidden-index")
            .setSettings(Settings.builder().put("index.hidden", true).build())
            .get());
        client().prepareIndex("hidden-index").setSource("foo", "bar").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();

        // default not visible to wildcard expansion
        SearchResponse searchResponse =
            client().prepareSearch(randomFrom("*", "_all", "h*", "*index")).setSize(1000).setQuery(QueryBuilders.matchAllQuery()).get();
        boolean matchedHidden = Arrays.stream(searchResponse.getHits().getHits()).anyMatch(hit -> "hidden-index".equals(hit.getIndex()));
        assertFalse(matchedHidden);

        // direct access allowed
        searchResponse = client().prepareSearch("hidden-index").setSize(1000).setQuery(QueryBuilders.matchAllQuery()).get();
        matchedHidden = Arrays.stream(searchResponse.getHits().getHits()).anyMatch(hit -> "hidden-index".equals(hit.getIndex()));
        assertTrue(matchedHidden);

        // with indices option to include hidden
        searchResponse = client().prepareSearch(randomFrom("*", "_all", "h*", "*index"))
            .setSize(1000)
            .setQuery(QueryBuilders.matchAllQuery())
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN)
            .get();
        matchedHidden = Arrays.stream(searchResponse.getHits().getHits()).anyMatch(hit -> "hidden-index".equals(hit.getIndex()));
        assertTrue(matchedHidden);

        // implicit based on use of pattern starting with . and a wildcard
        assertAcked(client().admin().indices().prepareCreate(".hidden-index")
            .setSettings(Settings.builder().put("index.hidden", true).build())
            .get());
        client().prepareIndex(".hidden-index").setSource("foo", "bar").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        searchResponse = client().prepareSearch(randomFrom(".*", ".hidden-*"))
            .setSize(1000)
            .setQuery(QueryBuilders.matchAllQuery())
            .get();
        matchedHidden = Arrays.stream(searchResponse.getHits().getHits()).anyMatch(hit -> ".hidden-index".equals(hit.getIndex()));
        assertTrue(matchedHidden);
    }

    public void testGlobalTemplatesDoNotApply() {
        assertAcked(client().admin().indices().preparePutTemplate("a_global_template").setPatterns(List.of("*"))
            .setMapping("foo", "type=text").get());
        assertAcked(client().admin().indices().preparePutTemplate("not_global_template").setPatterns(List.of("a*"))
            .setMapping("bar", "type=text").get());
        assertAcked(client().admin().indices().preparePutTemplate("specific_template").setPatterns(List.of("a_hidden_index"))
            .setMapping("baz", "type=text").get());
        assertAcked(client().admin().indices().preparePutTemplate("unused_template").setPatterns(List.of("not_used"))
            .setMapping("foobar", "type=text").get());

        assertAcked(client().admin().indices().prepareCreate("a_hidden_index")
            .setSettings(Settings.builder().put("index.hidden", true).build()).get());

        GetMappingsResponse mappingsResponse = client().admin().indices().prepareGetMappings("a_hidden_index").get();
        assertThat(mappingsResponse.mappings().size(), is(1));
        MappingMetaData mappingMetaData = mappingsResponse.mappings().get("a_hidden_index");
        assertNotNull(mappingMetaData);
        Map<String, Object> propertiesMap = (Map<String, Object>) mappingMetaData.getSourceAsMap().get("properties");
        assertNotNull(propertiesMap);
        assertThat(propertiesMap.size(), is(2));
        Map<String, Object> barMap = (Map<String, Object>) propertiesMap.get("bar");
        assertNotNull(barMap);
        assertThat(barMap.get("type"), is("text"));
        Map<String, Object> bazMap = (Map<String, Object>) propertiesMap.get("baz");
        assertNotNull(bazMap);
        assertThat(bazMap.get("type"), is("text"));
    }

    public void testGlobalTemplateCannotMakeIndexHidden() {
        InvalidIndexTemplateException invalidIndexTemplateException = expectThrows(InvalidIndexTemplateException.class,
            () -> client().admin().indices().preparePutTemplate("a_global_template")
                .setPatterns(List.of("*"))
                .setSettings(Settings.builder().put("index.hidden", randomBoolean()).build())
                .get());
        assertThat(invalidIndexTemplateException.getMessage(), containsString("global templates may not specify the setting index.hidden"));
    }

    public void testNonGlobalTemplateCanMakeIndexHidden() {
        assertAcked(client().admin().indices().preparePutTemplate("a_global_template")
            .setPatterns(List.of("my_hidden_pattern*"))
            .setMapping("foo", "type=text")
            .setSettings(Settings.builder().put("index.hidden", true).build())
            .get());
        assertAcked(client().admin().indices().prepareCreate("my_hidden_pattern1").get());
        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings("my_hidden_pattern1").get();
        assertThat(getSettingsResponse.getSetting("my_hidden_pattern1", "index.hidden"), is("true"));
    }
}
