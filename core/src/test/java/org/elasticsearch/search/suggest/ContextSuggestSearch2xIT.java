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

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.search.suggest.completion2x.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion2x.context.ContextBuilder;
import org.elasticsearch.search.suggest.completion2x.context.ContextMapping;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.carrotsearch.randomizedtesting.RandomizedTest.getRandom;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSuggestion;
import static org.elasticsearch.test.hamcrest.ElasticsearchGeoAssertions.assertDistance;
import static org.hamcrest.Matchers.containsString;

@SuppressCodecs("*") // requires custom completion format
public class ContextSuggestSearch2xIT extends ESIntegTestCase {

    private static final String INDEX = "test";
    private static final String TYPE = "testType";
    private static final String FIELD = "testField";
    private final Version PRE2X_VERSION = VersionUtils.randomVersionBetween(getRandom(), Version.V_2_0_0, Version.V_2_3_1);

    private static final String[][] HEROS = {
        { "Afari, Jamal", "Jamal Afari", "Jamal" },
        { "Allerdyce, St. John", "Allerdyce, John", "St. John", "St. John Allerdyce" },
        { "Beaubier, Jean-Paul", "Jean-Paul Beaubier", "Jean-Paul" },
        { "Beaubier, Jeanne-Marie", "Jeanne-Marie Beaubier", "Jeanne-Marie" },
        { "Braddock, Elizabeth \"Betsy\"", "Betsy", "Braddock, Elizabeth", "Elizabeth Braddock", "Elizabeth" },
        { "Cody Mushumanski gun Man", "the hunter", "gun man", "Cody Mushumanski" },
        { "Corbo, Adrian", "Adrian Corbo", "Adrian" },
        { "Corbo, Jared", "Jared Corbo", "Jared" },
        { "Creel, Carl \"Crusher\"", "Creel, Carl", "Crusher", "Carl Creel", "Carl" },
        { "Crichton, Lady Jacqueline Falsworth", "Lady Jacqueline Falsworth Crichton", "Lady Jacqueline Falsworth",
            "Jacqueline Falsworth" }, { "Crichton, Kenneth", "Kenneth Crichton", "Kenneth" },
        { "MacKenzie, Al", "Al MacKenzie", "Al" },
        { "MacPherran, Mary \"Skeeter\"", "Mary MacPherran \"Skeeter\"", "MacPherran, Mary", "Skeeter", "Mary MacPherran" },
        { "MacTaggert, Moira", "Moira MacTaggert", "Moira" }, { "Rasputin, Illyana", "Illyana Rasputin", "Illyana" },
        { "Rasputin, Mikhail", "Mikhail Rasputin", "Mikhail" }, { "Rasputin, Piotr", "Piotr Rasputin", "Piotr" },
        { "Smythe, Alistair", "Alistair Smythe", "Alistair" }, { "Smythe, Spencer", "Spencer Smythe", "Spencer" },
        { "Whitemane, Aelfyre", "Aelfyre Whitemane", "Aelfyre" }, { "Whitemane, Kofi", "Kofi Whitemane", "Kofi" } };

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testBasicGeo() throws Exception {
        assertAcked(prepareCreate(INDEX)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
            .addMapping(TYPE, createMapping(TYPE, ContextBuilder.location("st").precision("5km").neighbors(true))));
        ensureYellow();

        XContentBuilder source1 = jsonBuilder()
            .startObject()
            .startObject(FIELD)
            .array("input", "Hotel Amsterdam", "Amsterdam")
            .field("output", "Hotel Amsterdam in Berlin")
            .startObject("context").latlon("st", 52.529172, 13.407333).endObject()
            .endObject()
            .endObject();
        client().prepareIndex(INDEX, TYPE, "1").setSource(source1).execute().actionGet();

        XContentBuilder source2 = jsonBuilder()
            .startObject()
            .startObject(FIELD)
            .array("input", "Hotel Berlin", "Berlin")
            .field("output", "Hotel Berlin in Amsterdam")
            .startObject("context").latlon("st", 52.363389, 4.888695).endObject()
            .endObject()
            .endObject();
        client().prepareIndex(INDEX, TYPE, "2").setSource(source2).execute().actionGet();

        client().admin().indices().prepareRefresh(INDEX).get();

        String suggestionName = randomAsciiOfLength(10);
        CompletionSuggestionBuilder context = SuggestBuilders.completionSuggestion(FIELD).text("h").size(10).contexts(
            new CompletionSuggestionBuilder.Contexts2x().addGeoLocation("st", 52.52, 13.4));

        SearchResponse suggestResponse = client().prepareSearch(INDEX).suggest(
            new SuggestBuilder().addSuggestion(suggestionName, context)).execute().actionGet();

        assertEquals(suggestResponse.getSuggest().size(), 1);
        assertEquals("Hotel Amsterdam in Berlin", suggestResponse.getSuggest()
            .getSuggestion(suggestionName).iterator().next().getOptions().iterator().next().getText().string());
    }

    public void testMultiLevelGeo() throws Exception {
        assertAcked(prepareCreate(INDEX).setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
            .addMapping(TYPE, createMapping(TYPE, ContextBuilder.location("st")
                .precision(1)
                .precision(2)
                .precision(3)
                .precision(4)
                .precision(5)
                .precision(6)
                .precision(7)
                .precision(8)
                .precision(9)
                .precision(10)
                .precision(11)
                .precision(12)
                .neighbors(true))));
        ensureYellow();

        XContentBuilder source1 = jsonBuilder()
            .startObject()
            .startObject(FIELD)
            .array("input", "Hotel Amsterdam", "Amsterdam")
            .field("output", "Hotel Amsterdam in Berlin")
            .startObject("context").latlon("st", 52.529172, 13.407333).endObject()
            .endObject()
            .endObject();
        client().prepareIndex(INDEX, TYPE, "1").setSource(source1).execute().actionGet();

        client().admin().indices().prepareRefresh(INDEX).get();

        for (int precision = 1; precision <= 12; precision++) {
            String suggestionName = randomAsciiOfLength(10);
            CompletionSuggestionBuilder context = new CompletionSuggestionBuilder(FIELD).text("h").size(10)
                .contexts(new CompletionSuggestionBuilder.Contexts2x().addGeoLocation("st", 52.529172, 13.407333, precision));

            SearchResponse suggestResponse = client().prepareSearch(INDEX).suggest(
                new SuggestBuilder().addSuggestion(suggestionName, context)).execute().actionGet();
            assertEquals(suggestResponse.getSuggest().size(), 1);
            assertEquals("Hotel Amsterdam in Berlin", suggestResponse.getSuggest().getSuggestion(suggestionName).iterator().next()
                .getOptions().iterator().next().getText().string());
        }
    }

    public void testMappingIdempotency() throws Exception {
        final int nPrecision = randomIntBetween(4, 12);
        List<Integer> precisions = new ArrayList<>(nPrecision);
        for (int i = 0; i < nPrecision; i++) {
            precisions.add(i+1);
        }
        Collections.shuffle(precisions, getRandom());
        int[] precision = new int[nPrecision];
        for (int i = 0; i < precision.length; i++) {
            precision[i] = precisions.get(i);
        }
        XContentBuilder mapping = jsonBuilder().startObject().startObject(TYPE)
            .startObject("properties").startObject("completion")
            .field("type", "completion")
            .startObject("context")
            .startObject("location")
            .field("type", "geo")
            .field("precision", precision)
            .endObject()
            .endObject()
            .endObject().endObject()
            .endObject().endObject();

        assertAcked(prepareCreate(INDEX).setSettings(
            Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id)).addMapping(TYPE, mapping.string()));
        ensureYellow();

        Collections.shuffle(precisions, getRandom());
        for (int i = 0; i < precision.length; i++) {
            precision[i] = precisions.get(i);
        }
        mapping = jsonBuilder().startObject().startObject(TYPE)
            .startObject("properties").startObject("completion")
            .field("type", "completion")
            .startObject("context")
            .startObject("location")
            .field("type", "geo")
            .field("precision", precision)
            .endObject()
            .endObject()
            .endObject().endObject()
            .endObject().endObject();
        assertAcked(client().admin().indices().preparePutMapping(INDEX).setType(TYPE).setSource(mapping.string()).get());
    }


    public void testGeoField() throws Exception {

        XContentBuilder mapping = jsonBuilder();
        mapping.startObject();
        mapping.startObject(TYPE);
        mapping.startObject("properties");
        mapping.startObject("pin");
        mapping.field("type", "geo_point");
        mapping.endObject();
        mapping.startObject(FIELD);
        mapping.field("type", "completion");
        mapping.field("analyzer", "simple");

        mapping.startObject("context");
        mapping.value(ContextBuilder.location("st", 5, true).field("pin").build());
        mapping.endObject();

        mapping.endObject();
        mapping.endObject();
        mapping.endObject();
        mapping.endObject();

        assertAcked(prepareCreate(INDEX).setSettings(
            Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id)
        ).addMapping(TYPE, mapping));
        ensureYellow();

        XContentBuilder source1 = jsonBuilder()
            .startObject()
            .latlon("pin", 52.529172, 13.407333)
            .startObject(FIELD)
            .array("input", "Hotel Amsterdam", "Amsterdam")
            .field("output", "Hotel Amsterdam in Berlin")
            .startObject("context").endObject()
            .endObject()
            .endObject();
        client().prepareIndex(INDEX, TYPE, "1").setSource(source1).execute().actionGet();

        XContentBuilder source2 = jsonBuilder()
            .startObject()
            .latlon("pin", 52.363389, 4.888695)
            .startObject(FIELD)
            .array("input", "Hotel Berlin", "Berlin")
            .field("output", "Hotel Berlin in Amsterdam")
            .startObject("context").endObject()
            .endObject()
            .endObject();
        client().prepareIndex(INDEX, TYPE, "2").setSource(source2).execute().actionGet();

        refresh();

        String suggestionName = randomAsciiOfLength(10);
        CompletionSuggestionBuilder context = SuggestBuilders.completionSuggestion(FIELD).text("h").size(10)
            .contexts(new CompletionSuggestionBuilder.Contexts2x()
            .addGeoLocation("st", 52.52, 13.4));
        SearchResponse suggestResponse = client().prepareSearch(INDEX).suggest(
            new SuggestBuilder().addSuggestion(suggestionName, context)).execute().actionGet();

        assertEquals(suggestResponse.getSuggest().size(), 1);
        assertEquals("Hotel Amsterdam in Berlin", suggestResponse.getSuggest()
            .getSuggestion(suggestionName).iterator().next().getOptions().iterator().next().getText().string());
    }

    public void testSimpleGeo() throws Exception {
        String reinickendorf = "u337p3mp11e2";
        String pankow = "u33e0cyyjur4";
        String koepenick = "u33dm4f7fn40";
        String bernau = "u33etnjf1yjn";
        String berlin = "u33dc1v0xupz";
        String mitte = "u33dc0cpke4q";
        String steglitz = "u336m36rjh2p";
        String wilmersdorf = "u336wmw0q41s";
        String spandau = "u336uqek7gh6";
        String tempelhof = "u33d91jh3by0";
        String schoeneberg = "u336xdrkzbq7";
        String treptow = "u33d9unn7fp7";

        double precision = 100.0; // meters

        assertAcked(prepareCreate(INDEX)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
            .addMapping(TYPE, createMapping(TYPE, ContextBuilder.location("st").precision(precision).neighbors(true))));
        ensureYellow();

        String[] locations = { reinickendorf, pankow, koepenick, bernau, berlin, mitte, steglitz, wilmersdorf, spandau, tempelhof,
            schoeneberg, treptow };

        String[][] input = { { "pizza - reinickendorf", "pizza", "food" }, { "pizza - pankow", "pizza", "food" },
            { "pizza - koepenick", "pizza", "food" }, { "pizza - bernau", "pizza", "food" }, { "pizza - berlin", "pizza", "food" },
            { "pizza - mitte", "pizza - berlin mitte", "pizza", "food" },
            { "pizza - steglitz", "pizza - Berlin-Steglitz", "pizza", "food" }, { "pizza - wilmersdorf", "pizza", "food" },
            { "pizza - spandau", "spandau bei berlin", "pizza", "food" },
            { "pizza - tempelhof", "pizza - berlin-tempelhof", "pizza", "food" },
            { "pizza - schoeneberg", "pizza - schöneberg", "pizza - berlin schoeneberg", "pizza", "food" },
            { "pizza - treptow", "pizza", "food" } };

        for (int i = 0; i < locations.length; i++) {
            XContentBuilder source = jsonBuilder().startObject().startObject(FIELD).field("input", input[i])
                .startObject("context").field("st", locations[i]).endObject().field("payload", locations[i])
                .endObject().endObject();
            client().prepareIndex(INDEX, TYPE, "" + i).setSource(source).execute().actionGet();
        }

        refresh();

        assertGeoSuggestionsInRange(berlin, "pizza", precision);
        assertGeoSuggestionsInRange(reinickendorf, "pizza", precision);
        assertGeoSuggestionsInRange(spandau, "pizza", precision);
        assertGeoSuggestionsInRange(koepenick, "pizza", precision);
        assertGeoSuggestionsInRange(schoeneberg, "pizza", precision);
        assertGeoSuggestionsInRange(tempelhof, "pizza", precision);
        assertGeoSuggestionsInRange(bernau, "pizza", precision);
        assertGeoSuggestionsInRange(pankow, "pizza", precision);
        assertGeoSuggestionsInRange(mitte, "pizza", precision);
        assertGeoSuggestionsInRange(steglitz, "pizza", precision);
        assertGeoSuggestionsInRange(mitte, "pizza", precision);
        assertGeoSuggestionsInRange(wilmersdorf, "pizza", precision);
        assertGeoSuggestionsInRange(treptow, "pizza", precision);
    }

    public void testSimplePrefix() throws Exception {
        assertAcked(prepareCreate(INDEX)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
            .addMapping(TYPE, createMapping(TYPE, ContextBuilder.category("st"))));
        ensureYellow();

        for (int i = 0; i < HEROS.length; i++) {
            XContentBuilder source = jsonBuilder().startObject().startObject(FIELD).field("input", HEROS[i])
                .startObject("context").field("st", i%3).endObject()
                .startObject("payload").field("group", i % 3).field("id", i).endObject()
                .endObject().endObject();
            client().prepareIndex(INDEX, TYPE, "" + i).setSource(source).execute().actionGet();
        }

        refresh();

        assertPrefixSuggestions(0, "a", "Afari, Jamal", "Adrian Corbo", "Adrian");
        assertPrefixSuggestions(0, "b", "Beaubier, Jeanne-Marie");
        assertPrefixSuggestions(0, "c", "Corbo, Adrian", "Crichton, Lady Jacqueline Falsworth");
        assertPrefixSuggestions(0, "mary", "Mary MacPherran \"Skeeter\"", "Mary MacPherran");
        assertPrefixSuggestions(0, "s", "Skeeter", "Smythe, Spencer", "Spencer Smythe", "Spencer");
        assertPrefixSuggestions(1, "s", "St. John", "St. John Allerdyce");
        assertPrefixSuggestions(2, "s", "Smythe, Alistair");
        assertPrefixSuggestions(1, "w", "Whitemane, Aelfyre");
        assertPrefixSuggestions(2, "w", "Whitemane, Kofi");
    }

    public void testTypeCategoryIsActuallyCalledCategory() throws Exception {
        XContentBuilder mapping = jsonBuilder();
        mapping.startObject().startObject(TYPE).startObject("properties")
            .startObject("suggest_field").field("type", "completion")
            .startObject("context").startObject("color").field("type", "category").endObject().endObject()
            .endObject()
            .endObject().endObject().endObject();
        assertAcked(prepareCreate(INDEX)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
            .addMapping(TYPE, mapping));
        ensureYellow();
        XContentBuilder doc1 = jsonBuilder();
        doc1.startObject().startObject("suggest_field")
            .field("input", "backpack_red")
            .startObject("context").field("color", "red", "all_colors").endObject()
            .endObject().endObject();
        XContentBuilder doc2 = jsonBuilder();
        doc2.startObject().startObject("suggest_field")
            .field("input", "backpack_green")
            .startObject("context").field("color", "green", "all_colors").endObject()
            .endObject().endObject();

        client().prepareIndex(INDEX, TYPE, "1")
            .setSource(doc1).execute()
            .actionGet();
        client().prepareIndex(INDEX, TYPE, "2")
            .setSource(doc2).execute()
            .actionGet();

        refresh();
        getBackpackSuggestionAndCompare("all_colors", "backpack_red", "backpack_green");
        getBackpackSuggestionAndCompare("red", "backpack_red");
        getBackpackSuggestionAndCompare("green", "backpack_green");
        getBackpackSuggestionAndCompare("not_existing_color");

    }

    private void getBackpackSuggestionAndCompare(String contextValue, String... expectedText) {
        Set<String> expected = new HashSet<>();
        Collections.addAll(expected, expectedText);
        CompletionSuggestionBuilder context = SuggestBuilders.completionSuggestion("suggest_field")
            .text("back").size(10).contexts(
                new CompletionSuggestionBuilder.Contexts2x().addContextField("color", contextValue));
        SearchRequestBuilder suggestionRequest = client().prepareSearch(INDEX).suggest(
            new SuggestBuilder().addSuggestion("suggestion", context));
        SearchResponse suggestResponse = suggestionRequest.execute().actionGet();
        Suggest suggest = suggestResponse.getSuggest();
        assertEquals(suggest.size(), 1);
        for (Suggestion<? extends Entry<? extends Option>> s : suggest) {
            CompletionSuggestion suggestion = (CompletionSuggestion) s;
            for (CompletionSuggestion.Entry entry : suggestion) {
                List<CompletionSuggestion.Entry.Option> options = entry.getOptions();
                assertEquals(options.size(), expectedText.length);
                for (CompletionSuggestion.Entry.Option option : options) {
                    assertTrue(expected.contains(option.getText().string()));
                    expected.remove(option.getText().string());
                }
            }
        }
    }

    public void testBasic() throws Exception {
        assertAcked(prepareCreate(INDEX).
            setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
            .addMapping(TYPE, createMapping(TYPE, false, ContextBuilder.reference("st", "_type"),
                ContextBuilder.reference("nd", "_type"))));
        ensureYellow();

        client().prepareIndex(INDEX, TYPE, "1")
            .setSource(
                jsonBuilder().startObject().startObject(FIELD)
                    .startArray("input").value("my hotel").value("this hotel").endArray()
                    .startObject("context").endObject()
                    .field("payload", TYPE + "|" + TYPE).endObject().endObject()).execute()
            .actionGet();

        refresh();

        assertDoubleFieldSuggestions(TYPE, TYPE, "m", "my hotel");
    }

    public void testSimpleField() throws Exception {
        assertAcked(prepareCreate(INDEX)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
            .addMapping(TYPE, createMapping(TYPE, ContextBuilder.reference("st", "category"))));
        ensureYellow();

        for (int i = 0; i < HEROS.length; i++) {
            client().prepareIndex(INDEX, TYPE, "" + i)
                .setSource(
                    jsonBuilder().startObject().field("category", Integer.toString(i % 3)).startObject(FIELD)
                        .field("input", HEROS[i])
                        .startObject("context").endObject().field("payload", Integer.toString(i % 3))
                        .endObject().endObject()).execute().actionGet();
        }

        refresh();

        assertFieldSuggestions("0", "a", "Afari, Jamal", "Adrian Corbo", "Adrian");
        assertFieldSuggestions("0", "b", "Beaubier, Jeanne-Marie");
        assertFieldSuggestions("0", "c", "Corbo, Adrian", "Crichton, Lady Jacqueline Falsworth");
        assertFieldSuggestions("0", "mary", "Mary MacPherran \"Skeeter\"", "Mary MacPherran");
        assertFieldSuggestions("0", "s", "Skeeter", "Smythe, Spencer", "Spencer Smythe", "Spencer");
        assertFieldSuggestions("1", "s", "St. John", "St. John Allerdyce");
        assertFieldSuggestions("2", "s", "Smythe, Alistair");
        assertFieldSuggestions("1", "w", "Whitemane, Aelfyre");
        assertFieldSuggestions("2", "w", "Whitemane, Kofi");

    }

    // see issue #10987
    public void testEmptySuggestion() throws Exception {
        String mapping = jsonBuilder()
            .startObject()
            .startObject(TYPE)
            .startObject("properties")
            .startObject(FIELD)
            .field("type", "completion")
            .startObject("context")
            .startObject("type_context")
            .field("path", "_type")
            .field("type", "category")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .string();

        assertAcked(client().admin().indices().prepareCreate(INDEX)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
            .addMapping(TYPE, mapping).get());
        ensureGreen();

        client().prepareIndex(INDEX, TYPE, "1").setSource(FIELD, "")
            .setRefreshPolicy(IMMEDIATE).get();

    }

    public void testMultiValueField() throws Exception {
        assertAcked(prepareCreate(INDEX).setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
            .addMapping(TYPE, createMapping(TYPE, ContextBuilder.reference("st", "category"))));
        ensureYellow();

        for (int i = 0; i < HEROS.length; i++) {
            client().prepareIndex(INDEX, TYPE, "" + i)
                .setSource(
                    jsonBuilder().startObject().startArray("category").value(Integer.toString(i % 3)).value("other").endArray()
                        .startObject(FIELD).field("input", HEROS[i]).startObject("context").endObject()
                        .field("payload", Integer.toString(i % 3)).endObject().endObject()).execute().actionGet();
        }

        refresh();

        assertFieldSuggestions("0", "a", "Afari, Jamal", "Adrian Corbo", "Adrian");
        assertFieldSuggestions("0", "b", "Beaubier, Jeanne-Marie");
        assertFieldSuggestions("0", "c", "Corbo, Adrian", "Crichton, Lady Jacqueline Falsworth");
        assertFieldSuggestions("0", "mary", "Mary MacPherran \"Skeeter\"", "Mary MacPherran");
        assertFieldSuggestions("0", "s", "Skeeter", "Smythe, Spencer", "Spencer Smythe", "Spencer");
        assertFieldSuggestions("1", "s", "St. John", "St. John Allerdyce");
        assertFieldSuggestions("2", "s", "Smythe, Alistair");
        assertFieldSuggestions("1", "w", "Whitemane, Aelfyre");
        assertFieldSuggestions("2", "w", "Whitemane, Kofi");
    }

    public void testMultiContext() throws Exception {
        assertAcked(prepareCreate(INDEX)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
            .addMapping(TYPE, createMapping(TYPE, ContextBuilder.reference("st", "categoryA"),
                ContextBuilder.reference("nd", "categoryB"))));
        ensureYellow();

        for (int i = 0; i < HEROS.length; i++) {
            client().prepareIndex(INDEX, TYPE, "" + i)
                .setSource(
                    jsonBuilder().startObject().field("categoryA").value("" + (char) ('0' + (i % 3))).field("categoryB")
                        .value("" + (char) ('A' + (i % 3))).startObject(FIELD).field("input", HEROS[i])
                        .startObject("context").endObject().field("payload", ((char) ('0' + (i % 3))) + "" + (char) ('A' + (i % 3)))
                        .endObject().endObject()).execute().actionGet();
        }

        refresh();

        assertMultiContextSuggestions("0", "A", "a", "Afari, Jamal", "Adrian Corbo", "Adrian");
        assertMultiContextSuggestions("0", "A", "b", "Beaubier, Jeanne-Marie");
        assertMultiContextSuggestions("0", "A", "c", "Corbo, Adrian", "Crichton, Lady Jacqueline Falsworth");
        assertMultiContextSuggestions("0", "A", "mary", "Mary MacPherran \"Skeeter\"", "Mary MacPherran");
        assertMultiContextSuggestions("0", "A", "s", "Skeeter", "Smythe, Spencer", "Spencer Smythe", "Spencer");
        assertMultiContextSuggestions("1", "B", "s", "St. John", "St. John Allerdyce");
        assertMultiContextSuggestions("2", "C", "s", "Smythe, Alistair");
        assertMultiContextSuggestions("1", "B", "w", "Whitemane, Aelfyre");
        assertMultiContextSuggestions("2", "C", "w", "Whitemane, Kofi");
    }

    public void testMultiContextWithFuzzyLogic() throws Exception {
        assertAcked(prepareCreate(INDEX)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
            .addMapping(TYPE,
                createMapping(TYPE, ContextBuilder.reference("st", "categoryA"), ContextBuilder.reference("nd", "categoryB"))));
        ensureYellow();

        for (int i = 0; i < HEROS.length; i++) {
            String source = jsonBuilder().startObject().field("categoryA", "" + (char) ('0' + (i % 3)))
                .field("categoryB", "" + (char) ('a' + (i % 3))).startObject(FIELD).field("input", HEROS[i])
                .startObject("context").endObject().startObject("payload").field("categoryA", "" + (char) ('0' + (i % 3)))
                .field("categoryB", "" + (char) ('a' + (i % 3))).endObject().endObject().endObject().string();
            client().prepareIndex(INDEX, TYPE, "" + i).setSource(source).execute().actionGet();
        }

        refresh();

        String[] prefix1 = { "0", "1", "2" };
        String[] prefix2 = { "a", "b", "c" };
        String[] prefix3 = { "0", "1" };
        String[] prefix4 = { "a", "b" };

        assertContextWithFuzzySuggestions(prefix1, prefix2, "mary", "MacKenzie, Al", "MacPherran, Mary", "MacPherran, Mary \"Skeeter\"",
            "MacTaggert, Moira", "Mary MacPherran", "Mary MacPherran \"Skeeter\"");
        assertContextWithFuzzySuggestions(prefix1, prefix2, "mac", "Mikhail", "Mary MacPherran \"Skeeter\"", "MacTaggert, Moira",
            "Moira MacTaggert", "Moira", "MacKenzie, Al", "Mary MacPherran", "Mikhail Rasputin", "MacPherran, Mary",
            "MacPherran, Mary \"Skeeter\"");
        assertContextWithFuzzySuggestions(prefix3, prefix4, "mary", "MacPherran, Mary", "MacPherran, Mary \"Skeeter\"",
            "MacTaggert, Moira", "Mary MacPherran", "Mary MacPherran \"Skeeter\"");
        assertContextWithFuzzySuggestions(prefix3, prefix4, "mac", "MacPherran, Mary", "MacPherran, Mary \"Skeeter\"", "MacTaggert, Moira",
            "Mary MacPherran", "Mary MacPherran \"Skeeter\"", "Mikhail", "Mikhail Rasputin", "Moira", "Moira MacTaggert");
    }

    public void testSimpleType() throws Exception {
        String[] types = { TYPE + "A", TYPE + "B", TYPE + "C" };

        CreateIndexRequestBuilder createIndexRequestBuilder = prepareCreate(INDEX)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id));
        for (String type : types) {
            createIndexRequestBuilder.addMapping(type, createMapping(type, ContextBuilder.reference("st", "_type")));
        }
        assertAcked(createIndexRequestBuilder);
        ensureYellow();

        for (int i = 0; i < HEROS.length; i++) {
            String type = types[i % types.length];
            client().prepareIndex(INDEX, type, "" + i)
                .setSource(
                    jsonBuilder().startObject().startObject(FIELD).field("input", HEROS[i])
                        .startObject("context").endObject().field("payload", type).endObject().endObject()).execute().actionGet();
        }

        refresh();

        assertFieldSuggestions(types[0], "a", "Afari, Jamal", "Adrian Corbo", "Adrian");
        assertFieldSuggestions(types[0], "b", "Beaubier, Jeanne-Marie");
        assertFieldSuggestions(types[0], "c", "Corbo, Adrian", "Crichton, Lady Jacqueline Falsworth");
        assertFieldSuggestions(types[0], "mary", "Mary MacPherran \"Skeeter\"", "Mary MacPherran");
        assertFieldSuggestions(types[0], "s", "Skeeter", "Smythe, Spencer", "Spencer Smythe", "Spencer");
        assertFieldSuggestions(types[1], "s", "St. John", "St. John Allerdyce");
        assertFieldSuggestions(types[2], "s", "Smythe, Alistair");
        assertFieldSuggestions(types[1], "w", "Whitemane, Aelfyre");
        assertFieldSuggestions(types[2], "w", "Whitemane, Kofi");
    }

    // issue 5525, default location didnt work with lat/lon map, and did not set default location appropriately
    public void testGeoContextDefaultMapping() throws Exception {
        GeoPoint berlinAlexanderplatz = GeoPoint.fromGeohash("u33dc1");

        XContentBuilder xContentBuilder = jsonBuilder().startObject()
            .startObject("poi").startObject("properties").startObject("suggest")
            .field("type", "completion")
            .startObject("context").startObject("location")
            .field("type", "geo")
            .field("precision", "500m")
            .startObject("default").field("lat", berlinAlexanderplatz.lat()).field("lon", berlinAlexanderplatz.lon()).endObject()
            .endObject().endObject()
            .endObject().endObject().endObject()
            .endObject();

        assertAcked(prepareCreate(INDEX)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
            .addMapping("poi", xContentBuilder));
        ensureYellow();

        index(INDEX, "poi", "1", jsonBuilder().startObject()
            .startObject("suggest").field("input", "Berlin Alexanderplatz").endObject().endObject());
        refresh();

        final String suggestionName = "suggestion";
        CompletionSuggestionBuilder suggestionBuilder = SuggestBuilders.completionSuggestion("suggest").text("b").size(10)
            .contexts(new CompletionSuggestionBuilder.Contexts2x().addGeoLocation("location",
                berlinAlexanderplatz.lat(), berlinAlexanderplatz.lon()));
        SearchResponse suggestResponse = client().prepareSearch(INDEX).suggest(
            new SuggestBuilder().addSuggestion(suggestionName, suggestionBuilder)).get();
        assertSuggestion(suggestResponse.getSuggest(), 0, suggestionName, "Berlin Alexanderplatz");
    }

    // issue 5525, setting the path of a category context and then indexing a document without that field returned an error
    public void testThatMissingPrefixesForContextReturnException() throws Exception {
        XContentBuilder xContentBuilder = jsonBuilder().startObject()
            .startObject("service").startObject("properties").startObject("suggest")
            .field("type", "completion")
            .startObject("context").startObject("color")
            .field("type", "category")
            .field("path", "color")
            .endObject().endObject()
            .endObject().endObject().endObject()
            .endObject();

        assertAcked(prepareCreate(INDEX)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
            .addMapping("service", xContentBuilder));
        ensureYellow();

        // now index a document with color field
        index(INDEX, "service", "1", jsonBuilder().startObject()
            .field("color", "red").startObject("suggest").field("input", "backback").endObject().endObject());

        // now index a document without a color field
        try {
            index(INDEX, "service", "2", jsonBuilder().startObject()
                .startObject("suggest").field("input", "backback").endObject().endObject());
            fail("index operation was not supposed to be successful");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("one or more prefixes needed"));
        }
    }

    public void testThatCategoryDefaultWorks() throws Exception {
        XContentBuilder xContentBuilder = jsonBuilder().startObject()
            .startObject("item").startObject("properties").startObject("suggest")
            .field("type", "completion")
            .startObject("context").startObject("color")
            .field("type", "category").field("default", "red")
            .endObject().endObject()
            .endObject().endObject().endObject()
            .endObject();

        assertAcked(prepareCreate(INDEX)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
            .addMapping("item", xContentBuilder));
        ensureYellow();

        index(INDEX, "item", "1", jsonBuilder().startObject()
            .startObject("suggest").field("input", "Hoodie red").endObject().endObject());
        index(INDEX, "item", "2", jsonBuilder().startObject()
            .startObject("suggest").field("input", "Hoodie blue").startObject("context").field("color", "blue")
            .endObject().endObject().endObject());
        refresh();

        final String suggestionName = "suggestion";
        CompletionSuggestionBuilder suggestionBuilder = SuggestBuilders.completionSuggestion("suggest").text("h").size(10)
            .contexts(new CompletionSuggestionBuilder.Contexts2x().addContextField("color", "red"));
        SearchResponse suggestResponse = client().prepareSearch(INDEX).suggest(
            new SuggestBuilder().addSuggestion(suggestionName, suggestionBuilder)).get();
        assertSuggestion(suggestResponse.getSuggest(), 0, suggestionName, "Hoodie red");
    }

    public void testThatDefaultCategoryAndPathWorks() throws Exception {
        XContentBuilder xContentBuilder = jsonBuilder().startObject()
            .startObject("item").startObject("properties").startObject("suggest")
            .field("type", "completion")
            .startObject("context").startObject("color")
            .field("type", "category")
            .field("default", "red")
            .field("path", "color")
            .endObject().endObject()
            .endObject().endObject().endObject()
            .endObject();

        assertAcked(prepareCreate(INDEX)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
            .addMapping("item", xContentBuilder));
        ensureYellow();

        index(INDEX, "item", "1", jsonBuilder().startObject()
            .startObject("suggest").field("input", "Hoodie red").endObject().endObject());
        index(INDEX, "item", "2", jsonBuilder().startObject()
            .startObject("suggest").field("input", "Hoodie blue").endObject().field("color", "blue").endObject());
        refresh();

        final String suggestionName = "suggestion";
        CompletionSuggestionBuilder suggestionBuilder = SuggestBuilders.completionSuggestion("suggest").text("h").size(10)
            .contexts(new CompletionSuggestionBuilder.Contexts2x().addContextField("color", "red"));
        SearchResponse suggestResponse = client().prepareSearch(INDEX).suggest(
            new SuggestBuilder().addSuggestion(suggestionName, suggestionBuilder)).get();
        assertSuggestion(suggestResponse.getSuggest(), 0, suggestionName, "Hoodie red");
    }

    public void testThatGeoPrecisionIsWorking() throws Exception {
        XContentBuilder xContentBuilder = jsonBuilder().startObject()
            .startObject("item").startObject("properties").startObject("suggest")
            .field("type", "completion")
            .startObject("context").startObject("location")
            .field("type", "geo")
            .field("precision", 4) // this means geo hashes with a length of four are used, like u345
            .endObject().endObject()
            .endObject().endObject().endObject()
            .endObject();

        assertAcked(prepareCreate(INDEX)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
            .addMapping("item", xContentBuilder));
        ensureYellow();

        // lets create some locations by geohashes in different cells with the precision 4
        // this means, that poelchaustr is not a neighour to alexanderplatz, but they share the same prefix until the fourth char!
        GeoPoint alexanderplatz = GeoPoint.fromGeohash("u33dc1");
        GeoPoint poelchaustr = GeoPoint.fromGeohash("u33du5");
        GeoPoint dahlem = GeoPoint.fromGeohash("u336q"); // berlin dahlem, should be included with that precision
        GeoPoint middleOfNoWhere = GeoPoint.fromGeohash("u334"); // location for west from berlin, should not be included

        index(INDEX, "item", "1", jsonBuilder().startObject()
            .startObject("suggest").field("input", "Berlin Alexanderplatz").field("weight", 3).startObject("context")
            .startObject("location").field("lat", alexanderplatz.lat()).field("lon", alexanderplatz.lon()).endObject()
            .endObject().endObject().endObject());
        index(INDEX, "item", "2", jsonBuilder().startObject().startObject("suggest").field("input", "Berlin Poelchaustr.")
            .field("weight", 2).startObject("context").startObject("location").field("lat", poelchaustr.lat())
            .field("lon", poelchaustr.lon()).endObject().endObject().endObject().endObject());
        index(INDEX, "item", "3", jsonBuilder().startObject().startObject("suggest").field("input", "Berlin Far Away")
            .field("weight", 1).startObject("context").startObject("location").field("lat", middleOfNoWhere.lat())
            .field("lon", middleOfNoWhere.lon()).endObject().endObject().endObject().endObject());
        index(INDEX, "item", "4", jsonBuilder().startObject().startObject("suggest")
            .field("input", "Berlin Dahlem").field("weight", 1).startObject("context").startObject("location")
            .field("lat", dahlem.lat()).field("lon", dahlem.lon()).endObject().endObject().endObject().endObject());
        refresh();

        final String suggestionName = "suggestion";
        CompletionSuggestionBuilder suggestionBuilder = SuggestBuilders.completionSuggestion("suggest")
            .text("b").size(10).contexts(new CompletionSuggestionBuilder.Contexts2x().addGeoLocation("location",
                alexanderplatz.lat(), alexanderplatz.lon()));
        SearchResponse suggestResponse = client().prepareSearch(INDEX).suggest(
            new SuggestBuilder().addSuggestion(suggestionName, suggestionBuilder)).get();
        assertSuggestion(suggestResponse.getSuggest(), 0, suggestionName,
            "Berlin Alexanderplatz", "Berlin Poelchaustr.", "Berlin Dahlem");
    }

    public void testThatNeighborsCanBeExcluded() throws Exception {
        XContentBuilder xContentBuilder = jsonBuilder().startObject()
            .startObject("item").startObject("properties").startObject("suggest")
            .field("type", "completion")
            .startObject("context").startObject("location")
            .field("type", "geo")
            .field("precision", 6)
            .field("neighbors", false)
            .endObject().endObject()
            .endObject().endObject().endObject()
            .endObject();

        assertAcked(prepareCreate(INDEX)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
            .addMapping("item", xContentBuilder));
        ensureYellow();

        GeoPoint alexanderplatz = GeoPoint.fromGeohash("u33dc1");
        // does not look like it, but is a direct neighbor
        // this test would fail, if the precision was set 4, as then both cells would be the same, u33d
        GeoPoint cellNeighbourOfAlexanderplatz = GeoPoint.fromGeohash("u33dbc");

        index(INDEX, "item", "1", jsonBuilder().startObject()
            .startObject("suggest").field("input", "Berlin Alexanderplatz")
            .field("weight", 3).startObject("context").startObject("location")
            .field("lat", alexanderplatz.lat()).field("lon", alexanderplatz.lon())
            .endObject().endObject().endObject().endObject());
        index(INDEX, "item", "2", jsonBuilder().startObject()
            .startObject("suggest").field("input", "Berlin Hackescher Markt")
            .field("weight", 2).startObject("context").startObject("location")
            .field("lat", cellNeighbourOfAlexanderplatz.lat()).field("lon", cellNeighbourOfAlexanderplatz.lon())
            .endObject().endObject().endObject().endObject());
        refresh();

        final String suggestionName = "suggestion";
        CompletionSuggestionBuilder suggestionBuilder = SuggestBuilders.completionSuggestion("suggest").text("b").size(10)
            .contexts(new CompletionSuggestionBuilder.Contexts2x()
                .addGeoLocation("location", alexanderplatz.lat(), alexanderplatz.lon()));
        SearchResponse suggestResponse = client().prepareSearch(INDEX).suggest(
            new SuggestBuilder().addSuggestion(suggestionName, suggestionBuilder)).get();
        assertSuggestion(suggestResponse.getSuggest(), 0, suggestionName, "Berlin Alexanderplatz");
    }

    public void testThatGeoPathCanBeSelected() throws Exception {
        XContentBuilder xContentBuilder = jsonBuilder().startObject()
            .startObject("item").startObject("properties").startObject("suggest")
            .field("type", "completion")
            .startObject("context").startObject("location")
            .field("type", "geo")
            .field("precision", "5m")
            .field("path", "loc")
            .endObject().endObject()
            .endObject().endObject().endObject()
            .endObject();

        assertAcked(prepareCreate(INDEX)
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
            .addMapping("item", xContentBuilder));
        ensureYellow();

        GeoPoint alexanderplatz = GeoPoint.fromGeohash("u33dc1");
        index(INDEX, "item", "1", jsonBuilder().startObject()
            .startObject("suggest").field("input", "Berlin Alexanderplatz").endObject()
            .startObject("loc").field("lat", alexanderplatz.lat()).field("lon", alexanderplatz.lon()).endObject()
            .endObject());
        refresh();

        final String suggestionName = "suggestion";
        CompletionSuggestionBuilder suggestionBuilder = SuggestBuilders.completionSuggestion("suggest").text("b").size(10)
            .contexts(new CompletionSuggestionBuilder.Contexts2x()
                .addGeoLocation("location", alexanderplatz.lat(), alexanderplatz.lon()));
        SearchResponse suggestResponse = client().prepareSearch(INDEX).suggest(
            new SuggestBuilder().addSuggestion(suggestionName, suggestionBuilder)).get();
        assertSuggestion(suggestResponse.getSuggest(), 0, suggestionName, "Berlin Alexanderplatz");
    }

    public void testThatPrecisionIsRequired() throws Exception {
        XContentBuilder xContentBuilder = jsonBuilder().startObject()
            .startObject("item").startObject("properties").startObject("suggest")
            .field("type", "completion")
            .startObject("context").startObject("location")
            .field("type", "geo")
            .field("path", "loc")
            .endObject().endObject()
            .endObject().endObject().endObject()
            .endObject();

        try {
            assertAcked(prepareCreate(INDEX)
                .setSettings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, PRE2X_VERSION.id))
                .addMapping("item", xContentBuilder));
            fail("expected MapperParsingException");
        } catch (MapperParsingException expected) {}
    }

    public void assertGeoSuggestionsInRange(String location, String suggest, double precision) throws IOException {
        String suggestionName = randomAsciiOfLength(10);
        CompletionSuggestionBuilder context = SuggestBuilders.completionSuggestion(FIELD).text(suggest).size(10)
            .contexts(new CompletionSuggestionBuilder.Contexts2x().addGeoLocation("st", location));
        SearchRequestBuilder suggestionRequest = client().prepareSearch(INDEX)
            .suggest(new SuggestBuilder().addSuggestion(suggestionName, context));
        SearchResponse suggestResponse = suggestionRequest.execute().actionGet();

        Suggest suggest2 = suggestResponse.getSuggest();
        assertTrue(suggest2.iterator().hasNext());
        for (Suggestion<? extends Entry<? extends Option>> s : suggest2) {
            CompletionSuggestion suggestion = (CompletionSuggestion) s;
            assertTrue(suggestion.iterator().hasNext());
            for (CompletionSuggestion.Entry entry : suggestion) {
                List<CompletionSuggestion.Entry.Option> options = entry.getOptions();
                assertTrue(options.iterator().hasNext());
                for (CompletionSuggestion.Entry.Option option : options) {
                    String target = option.getPayloadAsString();
                    assertDistance(location, target, Matchers.lessThanOrEqualTo(precision));
                }
            }
        }
    }

    public void assertPrefixSuggestions(long prefix, String suggest, String... hits) throws IOException {
        String suggestionName = randomAsciiOfLength(10);
        CompletionSuggestionBuilder context = SuggestBuilders.completionSuggestion(FIELD).text(suggest)
            .size(hits.length + 1).contexts(new CompletionSuggestionBuilder.Contexts2x()
                .addCategory("st", Long.toString(prefix)));
        SearchResponse suggestResponse = client().prepareSearch(INDEX).suggest(
            new SuggestBuilder().addSuggestion(suggestionName, context)).execute().actionGet();
        ArrayList<String> suggestions = new ArrayList<>();
        Suggest suggest2 = suggestResponse.getSuggest();
        assertTrue(suggest2.iterator().hasNext());
        for (Suggestion<? extends Entry<? extends Option>> s : suggest2) {
            CompletionSuggestion suggestion = (CompletionSuggestion) s;
            for (CompletionSuggestion.Entry entry : suggestion) {
                List<CompletionSuggestion.Entry.Option> options = entry.getOptions();
                for (CompletionSuggestion.Entry.Option option : options) {
                    Map<String, Object> payload = option.getPayloadAsMap();
                    int group = (Integer) payload.get("group");
                    String text = option.getText().string();
                    assertEquals(prefix, group);
                    suggestions.add(text);
                }
            }
        }
        assertSuggestionsMatch(suggestions, hits);
    }

    public void assertContextWithFuzzySuggestions(String[] prefix1, String[] prefix2, String suggest, String... hits)
        throws IOException {
        String suggestionName = randomAsciiOfLength(10);
        CompletionSuggestionBuilder context = SuggestBuilders.completionSuggestion(FIELD).prefix(suggest, Fuzziness.TWO)
            .size(hits.length + 10).contexts(
                new CompletionSuggestionBuilder.Contexts2x().addContextField("st", prefix1).addContextField("nd", prefix2));
        SearchRequestBuilder suggestionRequest = client().prepareSearch(INDEX).suggest(
            new SuggestBuilder().addSuggestion(suggestionName, context));
        SearchResponse suggestResponse = suggestionRequest.execute().actionGet();

        ArrayList<String> suggestions = new ArrayList<>();

        Suggest suggest2 = suggestResponse.getSuggest();
        assertTrue(suggest2.iterator().hasNext());
        for (Suggestion<? extends Entry<? extends Option>> s : suggest2) {
            CompletionSuggestion suggestion = (CompletionSuggestion) s;
            for (CompletionSuggestion.Entry entry : suggestion) {
                List<CompletionSuggestion.Entry.Option> options = entry.getOptions();
                for (CompletionSuggestion.Entry.Option option : options) {
                    Map<String, Object> payload = option.getPayloadAsMap();
                    String text = option.getText().string();
                    assertThat(prefix1, Matchers.hasItemInArray(payload.get("categoryA")));
                    assertThat(prefix2, Matchers.hasItemInArray(payload.get("categoryB")));
                    suggestions.add(text);
                }
            }
        }

        assertSuggestionsMatch(suggestions, hits);
    }

    public void assertFieldSuggestions(String value, String suggest, String... hits) throws IOException {
        String suggestionName = randomAsciiOfLength(10);
        CompletionSuggestionBuilder context = SuggestBuilders.completionSuggestion(FIELD).text(suggest).size(10)
            .contexts(new CompletionSuggestionBuilder.Contexts2x().addContextField("st", value));
        SearchRequestBuilder suggestionRequest = client().prepareSearch(INDEX).suggest(
            new SuggestBuilder().addSuggestion(suggestionName, context));
        SearchResponse suggestResponse = suggestionRequest.execute().actionGet();

        ArrayList<String> suggestions = new ArrayList<>();

        Suggest suggest2 = suggestResponse.getSuggest();
        for (Suggestion<? extends Entry<? extends Option>> s : suggest2) {
            CompletionSuggestion suggestion = (CompletionSuggestion) s;
            for (CompletionSuggestion.Entry entry : suggestion) {
                List<CompletionSuggestion.Entry.Option> options = entry.getOptions();
                for (CompletionSuggestion.Entry.Option option : options) {
                    String payload = option.getPayloadAsString();
                    String text = option.getText().string();
                    assertEquals(value, payload);
                    suggestions.add(text);
                }
            }
        }
        assertSuggestionsMatch(suggestions, hits);
    }

    public void assertDoubleFieldSuggestions(String field1, String field2, String suggest, String... hits) throws IOException {
        String suggestionName = randomAsciiOfLength(10);
        CompletionSuggestionBuilder context = SuggestBuilders.completionSuggestion(FIELD).text(suggest).size(10)
            .contexts(new CompletionSuggestionBuilder.Contexts2x()
            .addContextField("st", field1).addContextField("nd", field2));
        SearchRequestBuilder suggestionRequest = client().prepareSearch(INDEX).suggest(
            new SuggestBuilder().addSuggestion(suggestionName, context));
        SearchResponse suggestResponse = suggestionRequest.execute().actionGet();
        ArrayList<String> suggestions = new ArrayList<>();

        Suggest suggest2 = suggestResponse.getSuggest();
        for (Suggestion<? extends Entry<? extends Option>> s : suggest2) {
            CompletionSuggestion suggestion = (CompletionSuggestion) s;
            for (CompletionSuggestion.Entry entry : suggestion) {
                List<CompletionSuggestion.Entry.Option> options = entry.getOptions();
                for (CompletionSuggestion.Entry.Option option : options) {
                    String payload = option.getPayloadAsString();
                    String text = option.getText().string();
                    assertEquals(field1 + "|" + field2, payload);
                    suggestions.add(text);
                }
            }
        }
        assertSuggestionsMatch(suggestions, hits);
    }

    public void assertMultiContextSuggestions(String value1, String value2, String suggest, String... hits) throws IOException {
        String suggestionName = randomAsciiOfLength(10);
        CompletionSuggestionBuilder context = SuggestBuilders.completionSuggestion(FIELD).text(suggest).size(10)
            .contexts(new CompletionSuggestionBuilder.Contexts2x()
            .addContextField("st", value1).addContextField("nd", value2));

        SearchRequestBuilder suggestionRequest = client().prepareSearch(INDEX).suggest(
            new SuggestBuilder().addSuggestion(suggestionName, context));
        SearchResponse suggestResponse = suggestionRequest.execute().actionGet();
        ArrayList<String> suggestions = new ArrayList<>();

        Suggest suggest2 = suggestResponse.getSuggest();
        for (Suggestion<? extends Entry<? extends Option>> s : suggest2) {
            CompletionSuggestion suggestion = (CompletionSuggestion) s;
            for (CompletionSuggestion.Entry entry : suggestion) {
                List<CompletionSuggestion.Entry.Option> options = entry.getOptions();
                for (CompletionSuggestion.Entry.Option option : options) {
                    String payload = option.getPayloadAsString();
                    String text = option.getText().string();
                    assertEquals(value1 + value2, payload);
                    suggestions.add(text);
                }
            }
        }
        assertSuggestionsMatch(suggestions, hits);
    }

    private void assertSuggestionsMatch(List<String> suggestions, String... hits) {
        boolean[] suggested = new boolean[hits.length];
        Arrays.sort(hits);
        Arrays.fill(suggested, false);
        int numSuggestions = 0;

        for (String suggestion : suggestions) {
            int hitpos = Arrays.binarySearch(hits, suggestion);

            assertEquals(hits[hitpos], suggestion);
            assertTrue(hitpos >= 0);
            assertTrue(!suggested[hitpos]);

            suggested[hitpos] = true;
            numSuggestions++;

        }
        assertEquals(hits.length, numSuggestions);
    }

    private XContentBuilder createMapping(String type, ContextBuilder<?>... context) throws IOException {
        return createMapping(type, false, context);
    }

    private XContentBuilder createMapping(String type, boolean preserveSeparators, ContextBuilder<?>... context)
        throws IOException {
        return createMapping(type, "simple", "simple", true, preserveSeparators, true, context);
    }

    private XContentBuilder createMapping(String type, String indexAnalyzer, String searchAnalyzer, boolean payloads,
                                          boolean preserveSeparators, boolean preservePositionIncrements,
                                          ContextBuilder<?>... contexts) throws IOException {
        XContentBuilder mapping = jsonBuilder();
        mapping.startObject();
        mapping.startObject(type);
        mapping.startObject("properties");
        mapping.startObject(FIELD);
        mapping.field("type", "completion");
        mapping.field("analyzer", indexAnalyzer);
        mapping.field("search_analyzer", searchAnalyzer);
        mapping.field("payloads", payloads);
        mapping.field("preserve_separators", preserveSeparators);
        mapping.field("preserve_position_increments", preservePositionIncrements);

        mapping.startObject("context");
        for (ContextBuilder<? extends ContextMapping> context : contexts) {
            mapping.value(context.build());
        }
        mapping.endObject();

        mapping.endObject();
        mapping.endObject();
        mapping.endObject();
        mapping.endObject();
        return mapping;
    }
}
