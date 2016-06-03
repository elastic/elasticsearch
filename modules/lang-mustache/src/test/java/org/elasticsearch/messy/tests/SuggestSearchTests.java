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

package org.elasticsearch.messy.tests;


import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGeneratorBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.search.suggest.SuggestBuilders.phraseSuggestion;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSuggestionPhraseCollateMatchExists;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSuggestionSize;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests for term and phrase suggestions.  Many of these tests many requests that vary only slightly from one another.  Where
 * possible these tests should declare for the first request, make the request, modify the configuration for the next request, make that
 * request, modify again, request again, etc.  This makes it very obvious what changes between requests.
 */
public class SuggestSearchTests extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(MustachePlugin.class);
    }

    public void testPhraseSuggesterCollate() throws InterruptedException, ExecutionException, IOException {
        CreateIndexRequestBuilder builder = prepareCreate("test").setSettings(Settings.builder()
                .put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, 1) // A single shard will help to keep the tests repeatable.
                .put("index.analysis.analyzer.text.tokenizer", "standard")
                .putArray("index.analysis.analyzer.text.filter", "lowercase", "my_shingle")
                .put("index.analysis.filter.my_shingle.type", "shingle")
                .put("index.analysis.filter.my_shingle.output_unigrams", true)
                .put("index.analysis.filter.my_shingle.min_shingle_size", 2)
                .put("index.analysis.filter.my_shingle.max_shingle_size", 3));

        XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("title")
                .field("type", "text")
                .field("analyzer", "text")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        assertAcked(builder.addMapping("type1", mapping));
        ensureGreen();

        List<String> titles = new ArrayList<>();

        titles.add("United States House of Representatives Elections in Washington 2006");
        titles.add("United States House of Representatives Elections in Washington 2005");
        titles.add("State");
        titles.add("Houses of Parliament");
        titles.add("Representative Government");
        titles.add("Election");

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (String title: titles) {
            builders.add(client().prepareIndex("test", "type1").setSource("title", title));
        }
        indexRandom(true, builders);

        // suggest without collate
        PhraseSuggestionBuilder suggest = phraseSuggestion("title")
                .addCandidateGenerator(new DirectCandidateGeneratorBuilder("title")
                        .suggestMode("always")
                        .maxTermFreq(.99f)
                        .size(10)
                        .maxInspections(200)
                )
                .confidence(0f)
                .maxErrors(2f)
                .shardSize(30000)
                .size(10);
        Suggest searchSuggest = searchSuggest("united states house of representatives elections in washington 2006", "title", suggest);
        assertSuggestionSize(searchSuggest, 0, 10, "title");

        // suggest with collate
        String filterString = XContentFactory.jsonBuilder()
                    .startObject()
                        .startObject("match_phrase")
                            .field("{{field}}", "{{suggestion}}")
                        .endObject()
                    .endObject()
                .string();
        PhraseSuggestionBuilder filteredQuerySuggest = suggest.collateQuery(filterString);
        filteredQuerySuggest.collateParams(Collections.singletonMap("field", "title"));
        searchSuggest = searchSuggest("united states house of representatives elections in washington 2006", "title", filteredQuerySuggest);
        assertSuggestionSize(searchSuggest, 0, 2, "title");

        // collate suggest with no result (boundary case)
        searchSuggest = searchSuggest("Elections of Representatives Parliament", "title", filteredQuerySuggest);
        assertSuggestionSize(searchSuggest, 0, 0, "title");

        NumShards numShards = getNumShards("test");

        // collate suggest with bad query
        String incorrectFilterString = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("test")
                        .field("title", "{{suggestion}}")
                    .endObject()
                .endObject()
                .string();
        PhraseSuggestionBuilder incorrectFilteredSuggest = suggest.collateQuery(incorrectFilterString);
        Map<String, SuggestionBuilder<?>> namedSuggestion = new HashMap<>();
        namedSuggestion.put("my_title_suggestion", incorrectFilteredSuggest);
        try {
            searchSuggest("united states house of representatives elections in washington 2006", numShards.numPrimaries, namedSuggestion);
            fail("Post query error has been swallowed");
        } catch(ElasticsearchException e) {
            // expected
        }

        // suggest with collation
        String filterStringAsFilter = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("match_phrase")
                .field("title", "{{suggestion}}")
                .endObject()
                .endObject()
                .string();

        PhraseSuggestionBuilder filteredFilterSuggest = suggest.collateQuery(filterStringAsFilter);
        searchSuggest = searchSuggest("united states house of representatives elections in washington 2006", "title",
            filteredFilterSuggest);
        assertSuggestionSize(searchSuggest, 0, 2, "title");

        // collate suggest with bad query
        String filterStr = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("pprefix")
                        .field("title", "{{suggestion}}")
                .endObject()
                .endObject()
                .string();

        PhraseSuggestionBuilder in = suggest.collateQuery(filterStr);
        try {
            searchSuggest("united states house of representatives elections in washington 2006", numShards.numPrimaries, namedSuggestion);
            fail("Post filter error has been swallowed");
        } catch(ElasticsearchException e) {
            //expected
        }

        // collate script failure due to no additional params
        String collateWithParams = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("{{query_type}}")
                    .field("{{query_field}}", "{{suggestion}}")
                .endObject()
                .endObject()
                .string();


        PhraseSuggestionBuilder phraseSuggestWithNoParams = suggest.collateQuery(collateWithParams);
        try {
            searchSuggest("united states house of representatives elections in washington 2006", numShards.numPrimaries, namedSuggestion);
            fail("Malformed query (lack of additional params) should fail");
        } catch (ElasticsearchException e) {
            // expected
        }

        // collate script with additional params
        Map<String, Object> params = new HashMap<>();
        params.put("query_type", "match_phrase");
        params.put("query_field", "title");

        PhraseSuggestionBuilder phraseSuggestWithParams = suggest.collateQuery(collateWithParams).collateParams(params);
        searchSuggest = searchSuggest("united states house of representatives elections in washington 2006", "title",
            phraseSuggestWithParams);
        assertSuggestionSize(searchSuggest, 0, 2, "title");

        // collate query request with prune set to true
        PhraseSuggestionBuilder phraseSuggestWithParamsAndReturn = suggest.collateQuery(collateWithParams).collateParams(params)
            .collatePrune(true);
        searchSuggest = searchSuggest("united states house of representatives elections in washington 2006", "title",
            phraseSuggestWithParamsAndReturn);
        assertSuggestionSize(searchSuggest, 0, 10, "title");
        assertSuggestionPhraseCollateMatchExists(searchSuggest, "title", 2);
    }

    protected Suggest searchSuggest(String name, SuggestionBuilder<?> suggestion) {
        return searchSuggest(null, name, suggestion);
    }

    protected Suggest searchSuggest(String suggestText, String name, SuggestionBuilder<?> suggestion) {
        Map<String, SuggestionBuilder<?>> map = new HashMap<>();
        map.put(name, suggestion);
        return searchSuggest(suggestText, 0, map);
    }

    protected Suggest searchSuggest(String suggestText, int expectShardsFailed, Map<String, SuggestionBuilder<?>> suggestions) {
        SearchRequestBuilder builder = client().prepareSearch().setSize(0);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        if (suggestText != null) {
            suggestBuilder.setGlobalText(suggestText);
        }
        for (Entry<String, SuggestionBuilder<?>> suggestion : suggestions.entrySet()) {
            suggestBuilder.addSuggestion(suggestion.getKey(), suggestion.getValue());
        }
        builder.suggest(suggestBuilder);
        SearchResponse actionGet = builder.execute().actionGet();
        assertThat(Arrays.toString(actionGet.getShardFailures()), actionGet.getFailedShards(), equalTo(expectShardsFailed));
        return actionGet.getSuggest();
    }
}
