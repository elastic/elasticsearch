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
package org.elasticsearch.search.suggest;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.google.common.collect.Lists;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.suggest.freetext.FreeTextStats;
import org.elasticsearch.search.suggest.freetext.FreeTextSuggestionBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class FreeTextSuggestSearchTests extends ElasticsearchIntegrationTest {

    private final String INDEX = randomAsciiOfLength(10).toLowerCase(Locale.ROOT);
    private final String TYPE = randomAsciiOfLength(10).toLowerCase(Locale.ROOT);
    private final String FIELD = randomAsciiOfLength(10).toLowerCase(Locale.ROOT);

    @Before
    public void setupIndex() throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder()
                // synonyms for testing
                .put("analysis.analyzer.suggest_analyzer_synonyms.type", "custom")
                .put("analysis.analyzer.suggest_analyzer_synonyms.tokenizer", "standard")
                .putArray("analysis.analyzer.suggest_analyzer_synonyms.filter", "standard", "lowercase", "my_synonyms")
                .put("analysis.filter.my_synonyms.type", "synonym")
                .putArray("analysis.filter.my_synonyms.synonyms", "copier,printer")
                // shingles for testing ngrams
                .put("analysis.analyzer.my_shingle.type", "custom")
                .put("analysis.analyzer.my_shingle.tokenizer", "standard")
                .putArray("analysis.analyzer.my_shingle.filter", "standard", "my_shingle_filter")
                .put("analysis.filter.my_shingle_filter.type", "shingle")
                .put("analysis.filter.my_shingle_filter.output_unigrams", false)
                .put("analysis.filter.my_shingle_filter.min_shingle_size", 2)
                .put("analysis.filter.my_shingle_filter.max_shingle_size", 3)
                // shingles for testing separator
                .put("analysis.analyzer.separator_shingle.type", "custom")
                .put("analysis.analyzer.separator_shingle.tokenizer", "standard")
                .putArray("analysis.analyzer.separator_shingle.filter", "standard", "separator_shingle_filter")
                .put("analysis.filter.separator_shingle_filter.type", "shingle")
                .put("analysis.filter.separator_shingle_filter.output_unigrams", false)
                .put("analysis.filter.separator_shingle_filter.min_shingle_size", 2)
                .put("analysis.filter.separator_shingle_filter.max_shingle_size", 3)
                .put("analysis.filter.separator_shingle_filter.token_separator", "|")
            .build();

        assertAcked(prepareCreate(INDEX).setSettings(settings));
    }

    @Test
    public void testSimpleSuggest() throws Exception {
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(jsonBuilder().startObject()
                .startObject(TYPE).startObject("properties").startObject(FIELD)
                .field("type", "string")
                .field("analyzer", "standard")
                .field("postings_format", "freetext")
                .endObject().endObject().endObject().endObject())
                .get();
        assertThat(putMappingResponse.isAcknowledged(), is(true));
        ensureGreen();

        index(INDEX, TYPE, "1", jsonBuilder().startObject().field(FIELD, "flashforge printer").endObject());
        index(INDEX, TYPE, "2", jsonBuilder().startObject().field(FIELD, "i need a new 3d printer").endObject());
        index(INDEX, TYPE, "3", jsonBuilder().startObject().field(FIELD, "i want another 3d printer").endObject());
        refresh();

        assertSuggestions("flashforge 3d p", "printer");
    }

    // TODO TEST DIFFERENT SEPARATOR
    // FIXME, need to find out how this works
    @Test
    @TestLogging("_root:DEBUG")
    public void testDifferentSeparator() throws Exception {
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(jsonBuilder().startObject()
                .startObject(TYPE).startObject("properties").startObject(FIELD)
                .field("type", "string")
                .field("analyzer", "separator_shingle")
                .field("postings_format", "freetext")
                .endObject().endObject().endObject().endObject())
                .get();
        assertThat(putMappingResponse.isAcknowledged(), is(true));
        ensureGreen();

        index(INDEX, TYPE, "1", jsonBuilder().startObject().field(FIELD, "B flashforge printer deluxe1").endObject());
        index(INDEX, TYPE, "2", jsonBuilder().startObject().field(FIELD, "B i need a new 3d printer deluxe2").endObject());
        index(INDEX, TYPE, "3", jsonBuilder().startObject().field(FIELD, "B i want another 3d printer deluxe3").endObject());
        refresh();

        SuggestResponse suggestResponse = client().prepareSuggest(INDEX)
                .addSuggestion(new FreeTextSuggestionBuilder("a").field(FIELD).separator("_").text("flashforge 3d p").size(10))
            .execute().actionGet();

        assertSuggestions(suggestResponse, false, "a", "3d_printer", "3d_printer_deluxe3", "3d_printer_deluxe2");
    }

    @Test
    public void testNgramsAreExtractedFromAnalyzer() throws Exception {
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(jsonBuilder().startObject()
                .startObject(TYPE).startObject("properties").startObject(FIELD)
                .field("type", "string")
                .field("analyzer", "my_shingle")
                .field("postings_format", "freetext")
                .endObject().endObject().endObject().endObject())
                .get();
        assertThat(putMappingResponse.isAcknowledged(), is(true));
        ensureGreen();

        index(INDEX, TYPE, "1", jsonBuilder().startObject().field(FIELD, "talk about long tail has now finished and was great").endObject());
        index(INDEX, TYPE, "2", jsonBuilder().startObject().field(FIELD, "awesome talk about the long tail in business and whatever").endObject());
        index(INDEX, TYPE, "3", jsonBuilder().startObject().field(FIELD, "this walk was way to long towards the sea and the atlantic").endObject());
        index(INDEX, TYPE, "4", jsonBuilder().startObject().field(FIELD, "long tail means money, money means rich, rich means awesome").endObject());
        refresh();

        assertSuggestionsNotInOrder("long t", "long tail", "long tail has", "long tail in", "long tail means", "long towards", "long towards the");
    }

    @Test
    public void testDifferentAnalyzers() throws Exception {
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(jsonBuilder().startObject()
                .startObject(TYPE).startObject("properties").startObject(FIELD)
                .field("type", "string")
                .field("search_analyzer", "standard")
                .field("index_analyzer", "suggest_analyzer_synonyms")
                .field("postings_format", "freetext")
                .endObject().endObject().endObject().endObject())
                .get();
        assertThat(putMappingResponse.isAcknowledged(), is(true));
        ensureGreen();

        index(INDEX, TYPE, "1", jsonBuilder().startObject().field(FIELD, "flashforge printer").endObject());
        index(INDEX, TYPE, "2", jsonBuilder().startObject().field(FIELD, "i need a new 3d printer").endObject());
        index(INDEX, TYPE, "3", jsonBuilder().startObject().field(FIELD, "i want another 3d printer").endObject());
        refresh();

        assertSuggestions("flashforge 3d c", "copier");
    }

    @Test
    public void testDifferentSegmentsNotContainingSuggestData() throws Exception {
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(jsonBuilder().startObject()
                .startObject(TYPE).startObject("properties").startObject(FIELD)
                .field("type", "string")
                .field("analyzer", "standard")
                .field("postings_format", "freetext")
                .endObject().endObject().endObject().endObject())
                .get();
        assertThat(putMappingResponse.isAcknowledged(), is(true));
        ensureGreen();

        index(INDEX, TYPE, "1", jsonBuilder().startObject().field(FIELD, "flashforge printer").endObject());
        index(INDEX, TYPE, "2", jsonBuilder().startObject().field(FIELD, "i need a new 3d printer").endObject());
        index(INDEX, TYPE, "3", jsonBuilder().startObject().field(FIELD, "i want another 3d printer").endObject());
        flushAndRefresh();
        index(INDEX, TYPE, "4", jsonBuilder().startObject().field("foo", "not relevant").endObject());
        flushAndRefresh();

        assertSuggestions("flashforge 3d p", "printer");
    }


    @Test
    public void testSuggestDoesNotFailWithEmptyData() throws Exception {
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(jsonBuilder().startObject()
                .startObject(TYPE).startObject("properties").startObject(FIELD)
                .field("type", "string")
                .field("analyzer", "standard")
                .field("postings_format", "freetext")
                .endObject().endObject().endObject().endObject())
                .get();
        assertThat(putMappingResponse.isAcknowledged(), is(true));
        ensureGreen();

        index(INDEX, TYPE, "1", jsonBuilder().startObject().field("foo", "flashforge printer").endObject());
        index(INDEX, TYPE, "2", jsonBuilder().startObject().field("foo", "i need a new 3d printer").endObject());
        index(INDEX, TYPE, "3", jsonBuilder().startObject().field("foo", "i want another 3d printer").endObject());
        index(INDEX, TYPE, "4", jsonBuilder().startObject().field("foo", "not relevant").endObject());
        flushAndRefresh();

        assertSuggestions("flashforge 3d p");
    }

    @Test
    public void testNonPostingFormatField() throws Exception {
        ensureGreen();
        index(INDEX, TYPE, "1", jsonBuilder().startObject().field("foo", "flashforge printer").endObject());
        refresh();

        SuggestResponse suggestResponse = client().prepareSuggest(INDEX).addSuggestion(new FreeTextSuggestionBuilder("broken").field("foo").text("f").size(10)).execute().actionGet();
        assertThat(suggestResponse.getFailedShards(), greaterThanOrEqualTo(1));
        assertThat(suggestResponse.getShardFailures()[0].reason(), containsString("Field [foo] is not configured with freetext postings format"));
    }

    @Test
    @TestLogging("_root:DEBUG")
    public void testNormalSearchOnFieldReturnsNoHitsAndNoException() throws Exception {
        ensureGreen();
        index(INDEX, TYPE, "1", jsonBuilder().startObject().field(FIELD, "flashforge printer").endObject());
        refresh();

        SearchResponse response = client().prepareSearch(INDEX).setQuery(QueryBuilders.termQuery(FIELD, "foo")).get();
        assertThat(response.getFailedShards(), greaterThanOrEqualTo(0));
        assertHitCount(response, 0);
    }

    @Test
    public void testMultipleSuggestInOneMappingWork() throws Exception {
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(jsonBuilder().startObject()
                .startObject(TYPE).startObject("properties").startObject(FIELD)
                .field("type", "string")
                .field("analyzer", "standard")
                .field("postings_format", "freetext")
                .endObject()
                .startObject(FIELD + "2")
                .field("type", "string")
                .field("analyzer", "standard")
                .field("postings_format", "freetext")
                .endObject() .endObject().endObject().endObject())
                .get();
        assertThat(putMappingResponse.isAcknowledged(), is(true));
        ensureGreen();

        index(INDEX, TYPE, "1", jsonBuilder().startObject().field(FIELD, "flashforge printer").field(FIELD + "2", "makibox printer").endObject());
        index(INDEX, TYPE, "2", jsonBuilder().startObject().field(FIELD, "i need a new 3d printer").field(FIELD + "2", "i need a new 3d printer").endObject());
        index(INDEX, TYPE, "3", jsonBuilder().startObject().field(FIELD, "i want another 3d printer").field(FIELD + "2", "i want another 3d printer").endObject());
        refresh();

        SuggestResponse suggestResponse = client().prepareSuggest(INDEX)
                .addSuggestion(new FreeTextSuggestionBuilder("a").field(FIELD).text("flashforge 3d p").size(10))
                .addSuggestion(new FreeTextSuggestionBuilder("b").field(FIELD + "2").text("makibox 3d p").size(10))
            .execute().actionGet();

        assertSuggestions(suggestResponse, "a", "printer");
        assertSuggestions(suggestResponse, "b", "printer");
    }


    @Test
    public void testMultiField() throws Exception {
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(jsonBuilder().startObject()
                .startObject(TYPE).startObject("properties").startObject(FIELD)
                .field("type", "string")
                .startObject("fields")
                .startObject("freetext")
                    .field("type", "string")
                    .field("postings_format", "freetext")
                    .field("analyzer", "standard")
                .endObject()
                .endObject()
                .endObject().endObject().endObject().endObject())
                .get();
        assertThat(putMappingResponse.isAcknowledged(), is(true));
        ensureGreen();

        index(INDEX, TYPE, "1", jsonBuilder().startObject().field(FIELD, "flashforge printer").endObject());
        index(INDEX, TYPE, "2", jsonBuilder().startObject().field(FIELD, "i need a new 3d printer").endObject());
        index(INDEX, TYPE, "3", jsonBuilder().startObject().field(FIELD, "i want another 3d printer").endObject());
        refresh();

        SuggestResponse suggestResponse = client().prepareSuggest(INDEX)
                .addSuggestion(new FreeTextSuggestionBuilder("a").field(FIELD + ".freetext").text("flashforge 3d p").size(10))
                .execute().actionGet();

        assertSuggestions(suggestResponse, "a", "printer");
    }

    @Test
    public void testUpgradingPostingsFormatShouldWork() throws Exception {
        ensureGreen();

        // create three segments here, mapping is implicit
        index(INDEX, TYPE, "1", jsonBuilder().startObject().field(FIELD, "flashforge printer").endObject());
        refresh();
        index(INDEX, TYPE, "2", jsonBuilder().startObject().field(FIELD, "i need a new 3d printer").endObject());
        refresh();
        index(INDEX, TYPE, "3", jsonBuilder().startObject().field(FIELD, "i want another 3d printer").endObject());
        refresh();

        // upgrade mapping to multifield
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(jsonBuilder().startObject()
                .startObject(TYPE).startObject("properties").startObject(FIELD)
                .field("type", "string")
                .field("postings_format", "freetext")
                .endObject().endObject().endObject().endObject())
                .get();
        assertThat(putMappingResponse.isAcknowledged(), is(true));
        ensureGreen();

        // assert no suggestions
        SuggestResponse suggestResponse = client().prepareSuggest(INDEX)
                .addSuggestion(new FreeTextSuggestionBuilder("a").field(FIELD).text("flashforge 3d p").size(10))
                .execute().actionGet();
        assertSuggestions(suggestResponse, "a");

        // call optimize and thus ensure everything is being written in the correct postings format now
        client().admin().indices().prepareOptimize(INDEX).setMaxNumSegments(1).setFlush(true).setWaitForMerge(true).get();
        refresh();

        suggestResponse = client().prepareSuggest(INDEX)
                .addSuggestion(new FreeTextSuggestionBuilder("a").field(FIELD).text("flashforge 3d p").size(10))
                .execute().actionGet();
        assertSuggestions(suggestResponse, "a", "printer");
    }

    @Test
    public void testDefaultAnalyzer() throws Exception {
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(jsonBuilder().startObject()
                .startObject(TYPE).startObject("properties").startObject(FIELD)
                .field("type", "string")
                .field("postings_format", "freetext")
                .endObject().endObject().endObject().endObject())
                .get();
        assertThat(putMappingResponse.isAcknowledged(), is(true));
        ensureGreen();

        index(INDEX, TYPE, "1", jsonBuilder().startObject().field(FIELD, "flashforge printer").endObject());
        index(INDEX, TYPE, "2", jsonBuilder().startObject().field(FIELD, "i need a new 3d printer").endObject());
        index(INDEX, TYPE, "3", jsonBuilder().startObject().field(FIELD, "i want another 3d printer").endObject());
        refresh();

        assertSuggestions("flashforge 3d p", "printer");
    }

    // TODO check if scoring values are not too high
    @Test
    public void testScoring() throws Exception {
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(jsonBuilder().startObject()
                .startObject(TYPE).startObject("properties").startObject(FIELD)
                .field("type", "string")
                .field("analyzer", "standard")
                .field("postings_format", "freetext")
                .endObject().endObject().endObject().endObject())
                .get();
        assertThat(putMappingResponse.isAcknowledged(), is(true));
        ensureGreen();

        index(INDEX, TYPE, "1", jsonBuilder().startObject().field(FIELD, "flashforge printer").endObject());
        index(INDEX, TYPE, "2", jsonBuilder().startObject().field(FIELD, "i need a new 3d printer power cable").endObject());
        index(INDEX, TYPE, "3", jsonBuilder().startObject().field(FIELD, "i want another 3d printer power cable").endObject());
        refresh();

        // both hits are in, next stop scores
        assertSuggestionsNotInOrder("flashforge 3d p", "power", "printer");

        SuggestResponse suggestResponse = client().prepareSuggest(INDEX).addSuggestion(new FreeTextSuggestionBuilder("a").field(FIELD).text("flashforge 3d p").size(10)).execute().actionGet();
        Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> suggestion = suggestResponse.getSuggest().getSuggestion("a");
        float powerScore = suggestion.getEntries().get(0).getOptions().get(0).getScore();
        float printerScore = suggestion.getEntries().get(0).getOptions().get(1).getScore();
        assertThat(powerScore, is(not(printerScore)));
    }

    @Test
    public void testMergingWorksWithUpdatedDocument() throws Exception {
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(jsonBuilder().startObject()
                .startObject(TYPE).startObject("properties").startObject(FIELD)
                .field("type", "string")
                .field("analyzer", "standard")
                .field("postings_format", "freetext")
                .endObject().endObject().endObject().endObject())
                .get();
        assertThat(putMappingResponse.isAcknowledged(), is(true));
        ensureGreen();

        index(INDEX, TYPE, "1", jsonBuilder().startObject().field(FIELD, "flashforge printer").endObject());
        index(INDEX, TYPE, "2", jsonBuilder().startObject().field(FIELD, "i need a new 3d printer").endObject());
        index(INDEX, TYPE, "3", jsonBuilder().startObject().field(FIELD, "i want another 3d printer").endObject());
        refresh();

        // merge needs to happen here with deleted document
        index(INDEX, TYPE, "1", jsonBuilder().startObject().field(FIELD, "flashforge printer").endObject());
        index(INDEX, TYPE, "2", jsonBuilder().startObject().field(FIELD, "i need a new 3d printer").endObject());
        index(INDEX, TYPE, "3", jsonBuilder().startObject().field(FIELD, "i want another 3d printer").endObject());
        client().admin().indices().prepareOptimize(INDEX).setMaxNumSegments(1).setFlush(true).get();
        refresh();

        assertSuggestions("flashforge 3d p", "printer");
    }

    @Test
    public void testThatStatsAreWorking() throws Exception {
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(jsonBuilder().startObject()
                .startObject(TYPE).startObject("properties").startObject(FIELD)
                .field("type", "string")
                .field("analyzer", "standard")
                .field("postings_format", "freetext")
                .endObject().endObject().endObject().endObject())
                .get();
        assertThat(putMappingResponse.isAcknowledged(), is(true));
        ensureGreen();

        String otherField = "testOtherField";
        index(INDEX, TYPE, "1", jsonBuilder().startObject().field(FIELD, "flashforge printer").field(otherField, "flashforge printer").endObject());
        index(INDEX, TYPE, "2", jsonBuilder().startObject().field(FIELD, "i need a new 3d printer").field(otherField, "i need a new 3d printer").endObject());
        index(INDEX, TYPE, "3", jsonBuilder().startObject().field(FIELD, "i want another 3d printer").field(otherField, "i want another 3d printer").endObject());
        refresh();

        assertSuggestions("flashforge 3d p", "printer");

        // Get all stats
        IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats(INDEX).setIndices(INDEX).setFreeText(true).get();
        FreeTextStats stats = indicesStatsResponse.getIndex(INDEX).getPrimaries().getFreeText();
        assertThat(stats, notNullValue());
        long totalSizeInBytes = stats.getSizeInBytes();
        assertThat(totalSizeInBytes, is(greaterThan(0L)));

        IndicesStatsResponse singleFieldStats = client().admin().indices().prepareStats(INDEX).setIndices(INDEX).setFreeText(true).setFreeTextFields(FIELD).get();
        long singleFieldSizeInBytes = singleFieldStats.getIndex(INDEX).getPrimaries().freeText.getFields().get(FIELD);
        IndicesStatsResponse otherFieldStats = client().admin().indices().prepareStats(INDEX).setIndices(INDEX).setFreeText(true).setFreeTextFields(otherField).get();
        long otherFieldSizeInBytes = otherFieldStats.getIndex(INDEX).getPrimaries().freeText.getFields().get(otherField);
        assertThat(singleFieldSizeInBytes + otherFieldSizeInBytes, is(totalSizeInBytes));

        // regexes
        IndicesStatsResponse regexFieldStats = client().admin().indices().prepareStats(INDEX).setIndices(INDEX).setFreeText(true).setFreeTextFields("*").get();
        long regexSizeInBytes = regexFieldStats.getIndex(INDEX).getPrimaries().freeText.getFields().get("*");
        assertThat(regexSizeInBytes, is(totalSizeInBytes));
    }


    // TODO MOVE ME INTO ElasticsearchSuggestAssertions and delete in CompletionSuggestSearchTests as well?
    public void assertSuggestions(String suggestion, String... suggestions) {
        String suggestionName = RandomStrings.randomAsciiOfLength(new Random(), 10);
        SuggestResponse suggestResponse = client().prepareSuggest(INDEX).addSuggestion(
                new FreeTextSuggestionBuilder(suggestionName).field(FIELD).text(suggestion).size(10)
        ).execute().actionGet();

        assertSuggestions(suggestResponse, suggestionName, suggestions);
    }

    public void assertSuggestionsNotInOrder(String suggestString, String... suggestions) {
        String suggestionName = RandomStrings.randomAsciiOfLength(new Random(), 10);
        SuggestResponse suggestResponse = client().prepareSuggest(INDEX).addSuggestion(
                new FreeTextSuggestionBuilder(suggestionName).field(FIELD).text(suggestString).size(10)
        ).execute().actionGet();

        assertSuggestions(suggestResponse, false, suggestionName, suggestions);
    }

    private void assertSuggestions(SuggestResponse suggestResponse, String name, String... suggestions) {
        assertSuggestions(suggestResponse, true, name, suggestions);
    }

    private void assertSuggestions(SuggestResponse suggestResponse, boolean suggestionOrderStrict, String name, String... suggestions) {
        assertNoFailures(suggestResponse);

        List<String> suggestionNames = Lists.newArrayList();
        for (Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> suggestion : Lists.newArrayList(suggestResponse.getSuggest().iterator())) {
            suggestionNames.add(suggestion.getName());
        }
        String expectFieldInResponseMsg = String.format(Locale.ROOT, "Expected suggestion named %s in response, got %s", name, suggestionNames);
        assertThat(expectFieldInResponseMsg, suggestResponse.getSuggest().getSuggestion(name), is(notNullValue()));

        Suggest.Suggestion<Suggest.Suggestion.Entry<Suggest.Suggestion.Entry.Option>> suggestion = suggestResponse.getSuggest().getSuggestion(name);

        List<String> suggestionList = getNames(suggestion.getEntries().get(0));
        List<Suggest.Suggestion.Entry.Option> options = suggestion.getEntries().get(0).getOptions();

        String assertMsg = String.format(Locale.ROOT, "Expected options %s length to be %s, but was %s", suggestionList, suggestions.length, options.size());
        assertThat(assertMsg, options.size(), is(suggestions.length));
        if (suggestionOrderStrict) {
            for (int i = 0; i < suggestions.length; i++) {
                String errMsg = String.format(Locale.ROOT, "Expected elem %s in list %s to be [%s] score: %s", i, suggestionList, suggestions[i], options.get(i).getScore());
                assertThat(errMsg, options.get(i).getText().toString(), is(suggestions[i]));
            }
        } else {
            for (String expectedSuggestion : suggestions) {
                String errMsg = String.format(Locale.ROOT, "Expected elem %s to be in list %s", expectedSuggestion, suggestionList);
                assertThat(errMsg, suggestionList, hasItem(expectedSuggestion));
            }
        }
    }

    private List<String> getNames(Suggest.Suggestion.Entry<Suggest.Suggestion.Entry.Option> suggestEntry) {
        List<String> names = Lists.newArrayList();
        for (Suggest.Suggestion.Entry.Option entry : suggestEntry.getOptions()) {
            names.add(entry.getText().string());
        }
        return names;
    }

}
