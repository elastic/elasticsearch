/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class HiddenIndexIT extends ESIntegTestCase {

    public void testHiddenIndexSearch() {
        assertAcked(indicesAdmin().prepareCreate("hidden-index").setSettings(Settings.builder().put("index.hidden", true).build()).get());
        client().prepareIndex("hidden-index").setSource("foo", "bar").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();

        // default not visible to wildcard expansion
        SearchResponse searchResponse = client().prepareSearch(randomFrom("*", "_all", "h*", "*index"))
            .setSize(1000)
            .setQuery(QueryBuilders.matchAllQuery())
            .get();
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
        assertAcked(indicesAdmin().prepareCreate(".hidden-index").setSettings(Settings.builder().put("index.hidden", true).build()).get());
        client().prepareIndex(".hidden-index").setSource("foo", "bar").setRefreshPolicy(RefreshPolicy.IMMEDIATE).get();
        searchResponse = client().prepareSearch(randomFrom(".*", ".hidden-*")).setSize(1000).setQuery(QueryBuilders.matchAllQuery()).get();
        matchedHidden = Arrays.stream(searchResponse.getHits().getHits()).anyMatch(hit -> ".hidden-index".equals(hit.getIndex()));
        assertTrue(matchedHidden);

        // make index not hidden
        updateIndexSettings(Settings.builder().put("index.hidden", false), "hidden-index");
        searchResponse = client().prepareSearch(randomFrom("*", "_all", "h*", "*index"))
            .setSize(1000)
            .setQuery(QueryBuilders.matchAllQuery())
            .get();
        matchedHidden = Arrays.stream(searchResponse.getHits().getHits()).anyMatch(hit -> "hidden-index".equals(hit.getIndex()));
        assertTrue(matchedHidden);
    }

    public void testGlobalTemplatesDoNotApply() {
        assertAcked(indicesAdmin().preparePutTemplate("a_global_template").setPatterns(List.of("*")).setMapping("foo", "type=text").get());
        assertAcked(
            indicesAdmin().preparePutTemplate("not_global_template").setPatterns(List.of("a*")).setMapping("bar", "type=text").get()
        );
        assertAcked(
            indicesAdmin().preparePutTemplate("specific_template")
                .setPatterns(List.of("a_hidden_index"))
                .setMapping("baz", "type=text")
                .get()
        );
        assertAcked(
            indicesAdmin().preparePutTemplate("unused_template").setPatterns(List.of("not_used")).setMapping("foobar", "type=text").get()
        );

        assertAcked(indicesAdmin().prepareCreate("a_hidden_index").setSettings(Settings.builder().put("index.hidden", true).build()).get());

        GetMappingsResponse mappingsResponse = indicesAdmin().prepareGetMappings("a_hidden_index").get();
        assertThat(mappingsResponse.mappings().size(), is(1));
        MappingMetadata mappingMetadata = mappingsResponse.mappings().get("a_hidden_index");
        assertNotNull(mappingMetadata);
        @SuppressWarnings("unchecked")
        Map<String, Object> propertiesMap = (Map<String, Object>) mappingMetadata.getSourceAsMap().get("properties");
        assertNotNull(propertiesMap);
        assertThat(propertiesMap.size(), is(2));
        @SuppressWarnings("unchecked")
        Map<String, Object> barMap = (Map<String, Object>) propertiesMap.get("bar");
        assertNotNull(barMap);
        assertThat(barMap.get("type"), is("text"));
        @SuppressWarnings("unchecked")
        Map<String, Object> bazMap = (Map<String, Object>) propertiesMap.get("baz");
        assertNotNull(bazMap);
        assertThat(bazMap.get("type"), is("text"));
    }

    public void testGlobalTemplateCannotMakeIndexHidden() {
        InvalidIndexTemplateException invalidIndexTemplateException = expectThrows(
            InvalidIndexTemplateException.class,
            () -> indicesAdmin().preparePutTemplate("a_global_template")
                .setPatterns(List.of("*"))
                .setSettings(Settings.builder().put("index.hidden", randomBoolean()).build())
                .get()
        );
        assertThat(invalidIndexTemplateException.getMessage(), containsString("global templates may not specify the setting index.hidden"));
    }

    public void testNonGlobalTemplateCanMakeIndexHidden() {
        assertAcked(
            indicesAdmin().preparePutTemplate("a_global_template")
                .setPatterns(List.of("my_hidden_pattern*"))
                .setMapping("foo", "type=text")
                .setSettings(Settings.builder().put("index.hidden", true).build())
                .get()
        );
        assertAcked(indicesAdmin().prepareCreate("my_hidden_pattern1").get());
        GetSettingsResponse getSettingsResponse = indicesAdmin().prepareGetSettings("my_hidden_pattern1").get();
        assertThat(getSettingsResponse.getSetting("my_hidden_pattern1", "index.hidden"), is("true"));
    }

    public void testAliasesForHiddenIndices() {
        final String hiddenIndex = "hidden-index";
        final String visibleAlias = "alias-visible";
        final String hiddenAlias = "alias-hidden";
        final String dotHiddenAlias = ".alias-hidden";

        assertAcked(indicesAdmin().prepareCreate(hiddenIndex).setSettings(Settings.builder().put("index.hidden", true).build()).get());

        assertAcked(
            indicesAdmin().prepareAliases().addAliasAction(IndicesAliasesRequest.AliasActions.add().index(hiddenIndex).alias(visibleAlias))
        );

        // The index should be returned here when queried by name or by wildcard because the alias is visible
        final GetAliasesRequestBuilder req = indicesAdmin().prepareGetAliases(visibleAlias);
        GetAliasesResponse response = req.get();
        assertThat(response.getAliases().get(hiddenIndex), hasSize(1));
        assertThat(response.getAliases().get(hiddenIndex).get(0).alias(), equalTo(visibleAlias));
        assertThat(response.getAliases().get(hiddenIndex).get(0).isHidden(), nullValue());

        response = indicesAdmin().prepareGetAliases("alias*").get();
        assertThat(response.getAliases().get(hiddenIndex), hasSize(1));
        assertThat(response.getAliases().get(hiddenIndex).get(0).alias(), equalTo(visibleAlias));
        assertThat(response.getAliases().get(hiddenIndex).get(0).isHidden(), nullValue());

        // Now try with a hidden alias
        assertAcked(
            indicesAdmin().prepareAliases()
                .addAliasAction(IndicesAliasesRequest.AliasActions.remove().index(hiddenIndex).alias(visibleAlias))
                .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(hiddenIndex).alias(hiddenAlias).isHidden(true))
        );

        // Querying by name directly should get the right result
        response = indicesAdmin().prepareGetAliases(hiddenAlias).get();
        assertThat(response.getAliases().get(hiddenIndex), hasSize(1));
        assertThat(response.getAliases().get(hiddenIndex).get(0).alias(), equalTo(hiddenAlias));
        assertThat(response.getAliases().get(hiddenIndex).get(0).isHidden(), equalTo(true));

        // querying by wildcard should get the right result because the indices options include hidden by default
        response = indicesAdmin().prepareGetAliases("alias*").get();
        assertThat(response.getAliases().get(hiddenIndex), hasSize(1));
        assertThat(response.getAliases().get(hiddenIndex).get(0).alias(), equalTo(hiddenAlias));
        assertThat(response.getAliases().get(hiddenIndex).get(0).isHidden(), equalTo(true));

        // But we should get no results if we specify indices options that don't include hidden
        response = indicesAdmin().prepareGetAliases("alias*").setIndicesOptions(IndicesOptions.strictExpandOpen()).get();
        assertThat(response.getAliases().get(hiddenIndex), nullValue());

        // Now try with a hidden alias that starts with a dot
        assertAcked(
            indicesAdmin().prepareAliases()
                .addAliasAction(IndicesAliasesRequest.AliasActions.remove().index(hiddenIndex).alias(hiddenAlias))
                .addAliasAction(IndicesAliasesRequest.AliasActions.add().index(hiddenIndex).alias(dotHiddenAlias).isHidden(true))
        );

        // Check that querying by dot-prefixed pattern returns the alias
        response = indicesAdmin().prepareGetAliases(".alias*").get();
        assertThat(response.getAliases().get(hiddenIndex), hasSize(1));
        assertThat(response.getAliases().get(hiddenIndex).get(0).alias(), equalTo(dotHiddenAlias));
        assertThat(response.getAliases().get(hiddenIndex).get(0).isHidden(), equalTo(true));
    }
}
