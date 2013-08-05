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
package org.elasticsearch.test.integration.search.suggest;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.google.common.collect.Lists;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.test.integration.AbstractSharedClusterTest;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.*;

public class CompletionSuggestSearchTests extends AbstractSharedClusterTest {

    private static final String INDEX = "test";
    private static final String TYPE = "testType";
    private static final String FIELD = "testField";

    @Test
    public void testSimple() throws Exception{
        createIndexAndMapping();
        String[][] input = {{"Foo Fighters"}, {"Foo Fighters"}, {"Foo Fighters"}, {"Foo Fighters"},
                            {"Generator", "Foo Fighters Generator"}, {"Learn to Fly", "Foo Fighters Learn to Fly" }, 
                            {"The Prodigy"}, {"The Prodigy"}, {"The Prodigy"}, {"Firestarter", "The Prodigy Firestarter"},
                            {"Turbonegro"}, {"Turbonegro"}, {"Get it on", "Turbonegro Get it on"}}; // work with frequencies
        for (int i = 0; i < input.length; i++) {
            client().prepareIndex(INDEX, TYPE, "" + i)
                    .setSource(jsonBuilder()
                            .startObject().startObject(FIELD)
                            .startArray("input").value(input[i]).endArray()
                            .endObject()
                            .endObject()
                    )
                    .execute().actionGet();
        }

        refresh();

        assertSuggestionsNotInOrder("f", "Foo Fighters", "Firestarter", "Foo Fighters Generator", "Foo Fighters Learn to Fly");
        assertSuggestionsNotInOrder("t", "The Prodigy", "Turbonegro", "Turbonegro Get it on", "The Prodigy Firestarter");
    }

    @Test
    public void testBasicPrefixSuggestion() throws Exception {
        createIndexAndMapping();
        for (int i = 0; i < 2; i++) {
            createData(i==0);
            assertSuggestions("f", "Firestarter - The Prodigy", "Foo Fighters", "Generator - Foo Fighters", "Learn to Fly - Foo Fighters");
            assertSuggestions("ge", "Generator - Foo Fighters", "Get it on - Turbonegro");
            assertSuggestions("ge", "Generator - Foo Fighters", "Get it on - Turbonegro");
            assertSuggestions("t", "The Prodigy", "Firestarter - The Prodigy", "Get it on - Turbonegro", "Turbonegro");
        }
    }

    @Test
    public void testThatWeightsAreWorking() throws Exception {
        createIndexAndMapping();

        List<String> similarNames = Lists.newArrayList("the", "The Prodigy", "The Verve", "The the");
        // the weight is 1000 divided by string length, so the results are easy to to check
        for (String similarName : similarNames) {
            client().prepareIndex(INDEX, TYPE, similarName).setSource(jsonBuilder()
                    .startObject().startObject(FIELD)
                    .startArray("input").value(similarName).endArray()
                    .field("weight", 1000 / similarName.length())
                    .endObject().endObject()
            ).get();
        }

        refresh();

        assertSuggestions("the", "the", "The the", "The Verve", "The Prodigy");
    }

    @Test
    public void testThatInputCanBeAStringInsteadOfAnArray() throws Exception {
        createIndexAndMapping();

        client().prepareIndex(INDEX, TYPE, "1").setSource(jsonBuilder()
                .startObject().startObject(FIELD)
                .field("input", "Foo Fighters")
                .field("output", "Boo Fighters")
                .endObject().endObject()
        ).get();

        refresh();

        assertSuggestions("f", "Boo Fighters");
    }

    @Test
    public void testThatPayloadsAreArbitraryJsonObjects() throws Exception {
        createIndexAndMapping();

        client().prepareIndex(INDEX, TYPE, "1").setSource(jsonBuilder()
                .startObject().startObject(FIELD)
                .startArray("input").value("Foo Fighters").endArray()
                .field("output", "Boo Fighters")
                .startObject("payload").field("foo", "bar").startArray("test").value("spam").value("eggs").endArray().endObject()
                .endObject().endObject()
        ).get();

        refresh();

        SuggestResponse suggestResponse = client().prepareSuggest(INDEX).addSuggestion(
                new CompletionSuggestionBuilder("testSuggestions").field(FIELD).text("foo").size(10)
        ).execute().actionGet();

        assertSuggestions(suggestResponse, "testSuggestions", "Boo Fighters");
        Suggest.Suggestion.Entry.Option option = suggestResponse.getSuggest().getSuggestion("testSuggestions").getEntries().get(0).getOptions().get(0);
        assertThat(option, is(instanceOf(CompletionSuggestion.Entry.Option.class)));
        CompletionSuggestion.Entry.Option prefixOption = (CompletionSuggestion.Entry.Option) option;
        assertThat(prefixOption.getPayload(), is(notNullValue()));

        // parse JSON
        Map<String, Object> jsonMap = JsonXContent.jsonXContent.createParser(prefixOption.getPayload()).mapAndClose();
        assertThat(jsonMap.size(), is(2));
        assertThat(jsonMap.get("foo").toString(), is("bar"));
        assertThat(jsonMap.get("test"), is(instanceOf(List.class)));
        List<String> listValues = (List<String>) jsonMap.get("test");
        assertThat(listValues, hasItems("spam", "eggs"));
    }

    @Test(expected = MapperException.class)
    public void testThatExceptionIsThrownWhenPayloadsAreDisabledButInIndexRequest() throws Exception {
        createIndexAndMapping("simple", "simple", false, false, true);

        client().prepareIndex(INDEX, TYPE, "1").setSource(jsonBuilder()
                .startObject().startObject(FIELD)
                .startArray("input").value("Foo Fighters").endArray()
                .field("output", "Boo Fighters")
                .startArray("payload").value("spam").value("eggs").endArray()
                .endObject().endObject()
        ).get();
    }

    @Test
    public void testDisabledPreserveSeperators() throws Exception {
        createIndexAndMapping("simple", "simple", true, false, true);

        client().prepareIndex(INDEX, TYPE, "1").setSource(jsonBuilder()
                .startObject().startObject(FIELD)
                .startArray("input").value("Foo Fighters").endArray()
                .field("weight", 10)
                .endObject().endObject()
        ).get();

        client().prepareIndex(INDEX, TYPE, "2").setSource(jsonBuilder()
                .startObject().startObject(FIELD)
                .startArray("input").value("Foof").endArray()
                .field("weight", 20)
                .endObject().endObject()
        ).get();

        refresh();

        assertSuggestions("foof", "Foof", "Foo Fighters");
    }

    @Test
    public void testEnabledPreserveSeperators() throws Exception {
        createIndexAndMapping("simple", "simple", true, true, true);

        client().prepareIndex(INDEX, TYPE, "1").setSource(jsonBuilder()
                .startObject().startObject(FIELD)
                .startArray("input").value("Foo Fighters").endArray()
                .endObject().endObject()
        ).get();

        client().prepareIndex(INDEX, TYPE, "2").setSource(jsonBuilder()
                .startObject().startObject(FIELD)
                .startArray("input").value("Foof").endArray()
                .endObject().endObject()
        ).get();

        refresh();

        assertSuggestions("foof", "Foof");
    }

    @Test
    public void testThatMultipleInputsAreSuppored() throws Exception {
        createIndexAndMapping("simple", "simple", false, false, true);

        client().prepareIndex(INDEX, TYPE, "1").setSource(jsonBuilder()
                .startObject().startObject(FIELD)
                .startArray("input").value("Foo Fighters").value("Fu Fighters").endArray()
                .field("output", "The incredible Foo Fighters")
                .endObject().endObject()
        ).get();

        refresh();

        assertSuggestions("foo", "The incredible Foo Fighters");
        assertSuggestions("fu", "The incredible Foo Fighters");
    }

    @Test
    public void testThatShortSyntaxIsWorking() throws Exception  {
        createIndexAndMapping();

        client().prepareIndex(INDEX, TYPE, "1").setSource(jsonBuilder()
                .startObject().startArray(FIELD)
                .value("The Prodigy Firestarter").value("Firestarter")
                .endArray().endObject()
        ).get();

        refresh();

        assertSuggestions("t", "The Prodigy Firestarter");
        assertSuggestions("f", "Firestarter");
    }

    @Test
    public void testThatDisablingPositionIncrementsWorkForStopwords() throws Exception {
        // analyzer which removes stopwords... so may not be the simple one
        createIndexAndMapping("standard", "standard", false, false, false);

        client().prepareIndex(INDEX, TYPE, "1").setSource(jsonBuilder()
                .startObject().startObject(FIELD)
                .startArray("input").value("The Beatles").endArray()
                .endObject().endObject()
        ).get();

        refresh();

        assertSuggestions("b", "The Beatles");
    }

    @Test
    public void testThatSynonymsWork() throws Exception {
        Settings.Builder settingsBuilder = settingsBuilder()
                .put("analysis.analyzer.suggest_analyzer_synonyms.type", "custom")
                .put("analysis.analyzer.suggest_analyzer_synonyms.tokenizer", "standard")
                .putArray("analysis.analyzer.suggest_analyzer_synonyms.filter", "standard", "lowercase", "my_synonyms")
                .put("analysis.filter.my_synonyms.type", "synonym")
                .putArray("analysis.filter.my_synonyms.synonyms", "foo,renamed");
        createIndexAndMappingAndSettings(settingsBuilder, "suggest_analyzer_synonyms", "suggest_analyzer_synonyms", false, false, true);

        client().prepareIndex(INDEX, TYPE, "1").setSource(jsonBuilder()
                .startObject().startObject(FIELD)
                .startArray("input").value("Foo Fighters").endArray()
                .endObject().endObject()
        ).get();

        refresh();

        // get suggestions for renamed
        assertSuggestions("r", "Foo Fighters");
    }

    @Test
    public void testThatUpgradeToMultiFieldWorks() throws Exception {
        client().admin().indices().prepareDelete().get();
        Settings.Builder settingsBuilder = createDefaultSettings();

        client().admin().indices().prepareCreate(INDEX).setSettings(settingsBuilder).get();
        ensureYellow();
        client().prepareIndex(INDEX, TYPE, "1").setRefresh(true).setSource(jsonBuilder().startObject().field(FIELD, "Foo Fighters").endObject()).get();

        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(jsonBuilder().startObject()
                .startObject(TYPE).startObject("properties")
                .startObject(FIELD)
                .field("type", "multi_field")
                .startObject("fields")
                .startObject(FIELD).field("type", "string").endObject()
                .startObject("suggest").field("type", "completion").field("index_analyzer", "simple").field("search_analyzer", "simple").endObject()
                .endObject()
                .endObject()
                .endObject().endObject()
                .endObject())
                .get();
        assertThat(putMappingResponse.isAcknowledged(), is(true));

        SuggestResponse suggestResponse = client().prepareSuggest(INDEX).addSuggestion(
                new CompletionSuggestionBuilder("suggs").field(FIELD + ".suggest").text("f").size(10)
        ).execute().actionGet();
        assertSuggestions(suggestResponse, "suggs");

        client().prepareIndex(INDEX, TYPE, "1").setRefresh(true).setSource(jsonBuilder().startObject().field(FIELD, "Foo Fighters").endObject()).get();
        waitForRelocation(ClusterHealthStatus.GREEN);

        SuggestResponse afterReindexingResponse = client().prepareSuggest(INDEX).addSuggestion(
                new CompletionSuggestionBuilder("suggs").field(FIELD + ".suggest").text("f").size(10)
        ).execute().actionGet();
        assertSuggestions(afterReindexingResponse, "suggs", "Foo Fighters");
    }

    public void assertSuggestions(String suggestion, String ... suggestions) {
        String suggestionName = RandomStrings.randomAsciiOfLength(new Random(), 10);
        SuggestResponse suggestResponse = client().prepareSuggest(INDEX).addSuggestion(
                new CompletionSuggestionBuilder(suggestionName).field(FIELD).text(suggestion).size(10)
        ).execute().actionGet();

        assertSuggestions(suggestResponse, suggestionName, suggestions);
    }

    public void assertSuggestionsNotInOrder(String suggestString, String ... suggestions) {
        String suggestionName = RandomStrings.randomAsciiOfLength(new Random(), 10);
        SuggestResponse suggestResponse = client().prepareSuggest(INDEX).addSuggestion(
                new CompletionSuggestionBuilder(suggestionName).field(FIELD).text(suggestString).size(10)
        ).execute().actionGet();

        assertSuggestions(suggestResponse, false, suggestionName, suggestions);
    }

    private void assertSuggestions(SuggestResponse suggestResponse, String name, String... suggestions) {
        assertSuggestions(suggestResponse, true, name, suggestions);
    }

    private void assertSuggestions(SuggestResponse suggestResponse, boolean suggestionOrderStrict, String name, String... suggestions) {
        assertNoFailures(suggestResponse);
        assertThat(suggestResponse.getSuggest().getSuggestion(name), is(notNullValue()));
        Suggest.Suggestion<Suggest.Suggestion.Entry<Suggest.Suggestion.Entry.Option>> suggestion = suggestResponse.getSuggest().getSuggestion(name);

        List<String> suggestionList = getNames(suggestion.getEntries().get(0));
        List<Suggest.Suggestion.Entry.Option> options = suggestion.getEntries().get(0).getOptions();

        String assertMsg = String.format(Locale.ROOT, "Expected options %s length to be %s, but was %s", suggestionList, suggestions.length, options.size());
        assertThat(assertMsg, options.size(), is(suggestions.length));
        if (suggestionOrderStrict) {
            for (int i = 0; i < suggestions.length; i++) {
                String errMsg = String.format(Locale.ROOT, "Expected elem %s in list %s to be [%s] score: %s", i, suggestionList, suggestions[i],  options.get(i).getScore());
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

    private void createIndexAndMapping() throws IOException {
        createIndexAndMapping("simple", "simple", true, false, true);
    }

    private void createIndexAndMappingAndSettings(Settings.Builder settingsBuilder, String indexAnalyzer, String searchAnalyzer, boolean payloads, boolean preserveSeparators, boolean preservePositionIncrements) throws IOException {
        client().admin().indices().prepareDelete().get();
        client().admin().indices().prepareCreate(INDEX)
                .setSettings(settingsBuilder)
                .get();
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(jsonBuilder().startObject()
                .startObject(TYPE).startObject("properties")
                .startObject(FIELD)
                .field("type", "completion")
                .field("index_analyzer", indexAnalyzer)
                .field("search_analyzer", searchAnalyzer)
                .field("payloads", payloads)
                .field("preserve_separators", preserveSeparators)
                .field("preserve_position_increments", preservePositionIncrements)
                .endObject()
                .endObject().endObject()
                .endObject())
                .get();
        assertThat(putMappingResponse.isAcknowledged(), is(true));
        ensureYellow();
    }

    private void createIndexAndMapping(String indexAnalyzer, String searchAnalyzer, boolean payloads, boolean preserveSeparators, boolean preservePositionIncrements) throws IOException {
        createIndexAndMappingAndSettings(createDefaultSettings(), indexAnalyzer, searchAnalyzer, payloads, preserveSeparators, preservePositionIncrements);
    }

    private ImmutableSettings.Builder createDefaultSettings() {
        int randomShardNumber = between(1, 5);
        int randomReplicaNumber = between(0, numberOfNodes()-1);
        return settingsBuilder().put(SETTING_NUMBER_OF_SHARDS, randomShardNumber).put(SETTING_NUMBER_OF_REPLICAS, randomReplicaNumber);
    }

    private void createData(boolean optimize) throws IOException, InterruptedException, ExecutionException {
        String[][] input = {{"Foo Fighters"}, {"Generator", "Foo Fighters Generator"}, {"Learn to Fly", "Foo Fighters Learn to Fly" }, {"The Prodigy"}, {"Firestarter", "The Prodigy Firestarter"}, {"Turbonegro"}, {"Get it on", "Turbonegro Get it on"}};
        String[] surface = {"Foo Fighters", "Generator - Foo Fighters", "Learn to Fly - Foo Fighters", "The Prodigy", "Firestarter - The Prodigy", "Turbonegro", "Get it on - Turbonegro"};
        int[] weight = {10, 9, 8, 12, 11, 6, 7};
        IndexRequestBuilder[] builders = new IndexRequestBuilder[input.length];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex(INDEX, TYPE, "" + i)
                    .setSource(jsonBuilder()
                            .startObject().startObject(FIELD)
                            .startArray("input").value(input[i]).endArray()
                            .field("output",surface[i])
                            .field("payload", "id: " + i)
                            .field("weight", 1) // WE FORCEFULLY INDEX A BOGUS WEIGHT 
                            .endObject()
                            .endObject()
                    );
        }
        indexRandom(INDEX, false, builders);

        for (int i = 0; i < builders.length; i++) { // add them again to make sure we deduplicate on the surface form
            builders[i] = client().prepareIndex(INDEX, TYPE, "n" + i)
                    .setSource(jsonBuilder()
                            .startObject().startObject(FIELD)
                            .startArray("input").value(input[i]).endArray()
                            .field("output",surface[i])
                            .field("payload", "id: " + i)
                            .field("weight", weight[i])
                            .endObject()
                            .endObject()
                    );
        }
        indexRandom(INDEX, false, builders);

        client().admin().indices().prepareRefresh(INDEX).execute().actionGet();
        if (optimize) {
            // make sure merging works just fine
            client().admin().indices().prepareFlush(INDEX).execute().actionGet();
            client().admin().indices().prepareOptimize(INDEX).execute().actionGet();
        }
    }
}
