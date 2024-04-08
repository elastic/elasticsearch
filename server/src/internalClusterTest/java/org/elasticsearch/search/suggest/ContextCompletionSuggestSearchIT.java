/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.suggest;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.suggest.CompletionSuggestSearchIT.CompletionMappingBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.search.suggest.completion.context.CategoryContextMapping;
import org.elasticsearch.search.suggest.completion.context.CategoryQueryContext;
import org.elasticsearch.search.suggest.completion.context.ContextBuilder;
import org.elasticsearch.search.suggest.completion.context.ContextMapping;
import org.elasticsearch.search.suggest.completion.context.GeoContextMapping;
import org.elasticsearch.search.suggest.completion.context.GeoQueryContext;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.core.IsEqual.equalTo;

@SuppressCodecs("*") // requires custom completion format
public class ContextCompletionSuggestSearchIT extends ESIntegTestCase {

    private final String INDEX = RandomStrings.randomAsciiOfLength(random(), 10).toLowerCase(Locale.ROOT);
    private final String FIELD = RandomStrings.randomAsciiOfLength(random(), 10).toLowerCase(Locale.ROOT);

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    public void testContextPrefix() throws Exception {
        LinkedHashMap<String, ContextMapping<?>> map = new LinkedHashMap<>();
        map.put("cat", ContextBuilder.category("cat").field("cat").build());
        boolean addAnotherContext = randomBoolean();
        if (addAnotherContext) {
            map.put("type", ContextBuilder.category("type").field("type").build());
        }
        final CompletionMappingBuilder mapping = new CompletionMappingBuilder().context(map);
        createIndexAndMapping(mapping);
        int numDocs = 10;
        List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder source = jsonBuilder().startObject()
                .startObject(FIELD)
                .field("input", "suggestion" + i)
                .field("weight", i + 1)
                .endObject()
                .field("cat", "cat" + i % 2);
            if (addAnotherContext) {
                source.field("type", "type" + i % 3);
            }
            source.endObject();
            indexRequestBuilders.add(prepareIndex(INDEX).setId("" + i).setSource(source));
        }
        indexRandom(true, indexRequestBuilders);
        CompletionSuggestionBuilder prefix = SuggestBuilders.completionSuggestion(FIELD)
            .prefix("sugg")
            .contexts(
                Collections.singletonMap(
                    "cat",
                    Collections.singletonList(CategoryQueryContext.builder().setCategory("cat").setPrefix(true).build())
                )
            );
        assertSuggestions("foo", prefix, "suggestion9", "suggestion8", "suggestion7", "suggestion6", "suggestion5");
    }

    public void testContextRegex() throws Exception {
        LinkedHashMap<String, ContextMapping<?>> map = new LinkedHashMap<>();
        map.put("cat", ContextBuilder.category("cat").field("cat").build());
        boolean addAnotherContext = randomBoolean();
        if (addAnotherContext) {
            map.put("type", ContextBuilder.category("type").field("type").build());
        }
        final CompletionMappingBuilder mapping = new CompletionMappingBuilder().context(map);
        createIndexAndMapping(mapping);
        int numDocs = 10;
        List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder source = jsonBuilder().startObject()
                .startObject(FIELD)
                .field("input", "sugg" + i + "estion")
                .field("weight", i + 1)
                .endObject()
                .field("cat", "cat" + i % 2);
            if (addAnotherContext) {
                source.field("type", "type" + i % 3);
            }
            source.endObject();
            indexRequestBuilders.add(prepareIndex(INDEX).setId("" + i).setSource(source));
        }
        indexRandom(true, indexRequestBuilders);
        CompletionSuggestionBuilder prefix = SuggestBuilders.completionSuggestion(FIELD)
            .regex("sugg.*es")
            .contexts(
                Collections.singletonMap(
                    "cat",
                    Collections.singletonList(CategoryQueryContext.builder().setCategory("cat").setPrefix(true).build())
                )
            );
        assertSuggestions("foo", prefix, "sugg9estion", "sugg8estion", "sugg7estion", "sugg6estion", "sugg5estion");
    }

    public void testContextFuzzy() throws Exception {
        LinkedHashMap<String, ContextMapping<?>> map = new LinkedHashMap<>();
        map.put("cat", ContextBuilder.category("cat").field("cat").build());
        boolean addAnotherContext = randomBoolean();
        if (addAnotherContext) {
            map.put("type", ContextBuilder.category("type").field("type").build());
        }
        final CompletionMappingBuilder mapping = new CompletionMappingBuilder().context(map);
        createIndexAndMapping(mapping);
        int numDocs = 10;
        List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder source = jsonBuilder().startObject()
                .startObject(FIELD)
                .field("input", "sugxgestion" + i)
                .field("weight", i + 1)
                .endObject()
                .field("cat", "cat" + i % 2);
            if (addAnotherContext) {
                source.field("type", "type" + i % 3);
            }
            source.endObject();
            indexRequestBuilders.add(prepareIndex(INDEX).setId("" + i).setSource(source));
        }
        indexRandom(true, indexRequestBuilders);
        CompletionSuggestionBuilder prefix = SuggestBuilders.completionSuggestion(FIELD)
            .prefix("sugg", Fuzziness.ONE)
            .contexts(
                Collections.singletonMap(
                    "cat",
                    Collections.singletonList(CategoryQueryContext.builder().setCategory("cat").setPrefix(true).build())
                )
            );
        assertSuggestions("foo", prefix, "sugxgestion9", "sugxgestion8", "sugxgestion7", "sugxgestion6", "sugxgestion5");
    }

    public void testContextFilteringWorksWithUTF8Categories() throws Exception {
        CategoryContextMapping contextMapping = ContextBuilder.category("cat").field("cat").build();
        LinkedHashMap<String, ContextMapping<?>> map = new LinkedHashMap<>(Collections.singletonMap("cat", contextMapping));
        final CompletionMappingBuilder mapping = new CompletionMappingBuilder().context(map);
        createIndexAndMapping(mapping);
        DocWriteResponse indexResponse = prepareIndex(INDEX).setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .startObject(FIELD)
                    .field("input", "suggestion")
                    .endObject()
                    .field("cat", "ctx\\u00e4")
                    .endObject()
            )
            .get();
        assertThat(indexResponse.status(), equalTo(RestStatus.CREATED));
        assertNoFailures(indicesAdmin().prepareRefresh(INDEX).get());
        CompletionSuggestionBuilder contextSuggestQuery = SuggestBuilders.completionSuggestion(FIELD)
            .prefix("sugg")
            .contexts(
                Collections.singletonMap("cat", Collections.singletonList(CategoryQueryContext.builder().setCategory("ctx\\u00e4").build()))
            );
        assertSuggestions("foo", contextSuggestQuery, "suggestion");
    }

    public void testSingleContextFiltering() throws Exception {
        CategoryContextMapping contextMapping = ContextBuilder.category("cat").field("cat").build();
        LinkedHashMap<String, ContextMapping<?>> map = new LinkedHashMap<>(Collections.singletonMap("cat", contextMapping));
        final CompletionMappingBuilder mapping = new CompletionMappingBuilder().context(map);
        createIndexAndMapping(mapping);
        int numDocs = 10;
        List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            indexRequestBuilders.add(
                prepareIndex(INDEX).setId("" + i)
                    .setSource(
                        jsonBuilder().startObject()
                            .startObject(FIELD)
                            .field("input", "suggestion" + i)
                            .field("weight", i + 1)
                            .endObject()
                            .field("cat", "cat" + i % 2)
                            .endObject()
                    )
            );
        }
        indexRandom(true, indexRequestBuilders);
        CompletionSuggestionBuilder prefix = SuggestBuilders.completionSuggestion(FIELD)
            .prefix("sugg")
            .contexts(
                Collections.singletonMap("cat", Collections.singletonList(CategoryQueryContext.builder().setCategory("cat0").build()))
            );

        assertSuggestions("foo", prefix, "suggestion8", "suggestion6", "suggestion4", "suggestion2", "suggestion0");
    }

    public void testSingleContextBoosting() throws Exception {
        CategoryContextMapping contextMapping = ContextBuilder.category("cat").field("cat").build();
        LinkedHashMap<String, ContextMapping<?>> map = new LinkedHashMap<>(Collections.singletonMap("cat", contextMapping));
        final CompletionMappingBuilder mapping = new CompletionMappingBuilder().context(map);
        createIndexAndMapping(mapping);
        int numDocs = 10;
        List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            indexRequestBuilders.add(
                prepareIndex(INDEX).setId("" + i)
                    .setSource(
                        jsonBuilder().startObject()
                            .startObject(FIELD)
                            .field("input", "suggestion" + i)
                            .field("weight", i + 1)
                            .endObject()
                            .field("cat", "cat" + i % 2)
                            .endObject()
                    )
            );
        }
        indexRandom(true, indexRequestBuilders);
        CompletionSuggestionBuilder prefix = SuggestBuilders.completionSuggestion(FIELD)
            .prefix("sugg")
            .contexts(
                Collections.singletonMap(
                    "cat",
                    Arrays.asList(
                        CategoryQueryContext.builder().setCategory("cat0").setBoost(3).build(),
                        CategoryQueryContext.builder().setCategory("cat1").build()
                    )
                )
            );
        assertSuggestions("foo", prefix, "suggestion8", "suggestion6", "suggestion4", "suggestion9", "suggestion2");
    }

    public void testMultiContextFiltering() throws Exception {
        LinkedHashMap<String, ContextMapping<?>> map = new LinkedHashMap<>();
        map.put("cat", ContextBuilder.category("cat").field("cat").build());
        map.put("type", ContextBuilder.category("type").field("type").build());
        final CompletionMappingBuilder mapping = new CompletionMappingBuilder().context(map);
        createIndexAndMapping(mapping);
        int numDocs = 10;
        List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder source = jsonBuilder().startObject()
                .startObject(FIELD)
                .field("input", "suggestion" + i)
                .field("weight", i + 1)
                .endObject()
                .field("cat", "cat" + i % 2)
                .field("type", "type" + i % 4)
                .endObject();
            indexRequestBuilders.add(prepareIndex(INDEX).setId("" + i).setSource(source));
        }
        indexRandom(true, indexRequestBuilders);

        // filter only on context cat
        CompletionSuggestionBuilder catFilterSuggest = SuggestBuilders.completionSuggestion(FIELD).prefix("sugg");
        catFilterSuggest.contexts(
            Collections.singletonMap("cat", Collections.singletonList(CategoryQueryContext.builder().setCategory("cat0").build()))
        );
        assertSuggestions("foo", catFilterSuggest, "suggestion8", "suggestion6", "suggestion4", "suggestion2", "suggestion0");

        // filter only on context type
        CompletionSuggestionBuilder typeFilterSuggest = SuggestBuilders.completionSuggestion(FIELD).prefix("sugg");
        typeFilterSuggest.contexts(
            Collections.singletonMap(
                "type",
                Arrays.asList(
                    CategoryQueryContext.builder().setCategory("type2").build(),
                    CategoryQueryContext.builder().setCategory("type1").build()
                )
            )
        );
        assertSuggestions("foo", typeFilterSuggest, "suggestion9", "suggestion6", "suggestion5", "suggestion2", "suggestion1");
    }

    public void testMultiContextBoosting() throws Exception {
        LinkedHashMap<String, ContextMapping<?>> map = new LinkedHashMap<>();
        map.put("cat", ContextBuilder.category("cat").field("cat").build());
        map.put("type", ContextBuilder.category("type").field("type").build());
        final CompletionMappingBuilder mapping = new CompletionMappingBuilder().context(map);
        createIndexAndMapping(mapping);
        int numDocs = 10;
        List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder source = jsonBuilder().startObject()
                .startObject(FIELD)
                .field("input", "suggestion" + i)
                .field("weight", i + 1)
                .endObject()
                .field("cat", "cat" + i % 2)
                .field("type", "type" + i % 4)
                .endObject();
            indexRequestBuilders.add(prepareIndex(INDEX).setId("" + i).setSource(source));
        }
        indexRandom(true, indexRequestBuilders);

        // boost only on context cat
        CompletionSuggestionBuilder catBoostSuggest = SuggestBuilders.completionSuggestion(FIELD).prefix("sugg");
        catBoostSuggest.contexts(
            Collections.singletonMap(
                "cat",
                Arrays.asList(
                    CategoryQueryContext.builder().setCategory("cat0").setBoost(3).build(),
                    CategoryQueryContext.builder().setCategory("cat1").build()
                )
            )
        );
        assertSuggestions("foo", catBoostSuggest, "suggestion8", "suggestion6", "suggestion4", "suggestion9", "suggestion2");

        // boost only on context type
        CompletionSuggestionBuilder typeBoostSuggest = SuggestBuilders.completionSuggestion(FIELD).prefix("sugg");
        typeBoostSuggest.contexts(
            Collections.singletonMap(
                "type",
                Arrays.asList(
                    CategoryQueryContext.builder().setCategory("type2").setBoost(2).build(),
                    CategoryQueryContext.builder().setCategory("type1").setBoost(4).build()
                )
            )
        );
        assertSuggestions("foo", typeBoostSuggest, "suggestion9", "suggestion5", "suggestion6", "suggestion1", "suggestion2");

        // boost on both contexts
        CompletionSuggestionBuilder multiContextBoostSuggest = SuggestBuilders.completionSuggestion(FIELD).prefix("sugg");
        // query context order should never matter
        Map<String, List<? extends ToXContent>> contextMap = new HashMap<>();
        contextMap.put(
            "type",
            Arrays.asList(
                CategoryQueryContext.builder().setCategory("type2").setBoost(2).build(),
                CategoryQueryContext.builder().setCategory("type1").setBoost(4).build()
            )
        );
        contextMap.put(
            "cat",
            Arrays.asList(
                CategoryQueryContext.builder().setCategory("cat0").setBoost(3).build(),
                CategoryQueryContext.builder().setCategory("cat1").build()
            )
        );
        multiContextBoostSuggest.contexts(contextMap);
        // the score of each suggestion is the maximum score among the matching contexts
        assertSuggestions("foo", multiContextBoostSuggest, "suggestion9", "suggestion8", "suggestion5", "suggestion6", "suggestion4");
    }

    public void testSeveralContexts() throws Exception {
        LinkedHashMap<String, ContextMapping<?>> map = new LinkedHashMap<>();
        final int numContexts = randomIntBetween(2, 5);
        for (int i = 0; i < numContexts; i++) {
            map.put("type" + i, ContextBuilder.category("type" + i).field("type" + i).build());
        }
        final CompletionMappingBuilder mapping = new CompletionMappingBuilder().context(map);
        createIndexAndMapping(mapping);
        int numDocs = randomIntBetween(10, 200);
        List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder source = jsonBuilder().startObject()
                .startObject(FIELD)
                .field("input", "suggestion" + i)
                .field("weight", numDocs - i)
                .endObject();
            for (int c = 0; c < numContexts; c++) {
                source.field("type" + c, "type" + c + i % 4);
            }
            source.endObject();
            indexRequestBuilders.add(prepareIndex(INDEX).setId("" + i).setSource(source));
        }
        indexRandom(true, indexRequestBuilders);

        CompletionSuggestionBuilder prefix = SuggestBuilders.completionSuggestion(FIELD)
            .prefix("sugg")
            .contexts(
                Collections.singletonMap(
                    "type0",
                    Collections.singletonList(CategoryQueryContext.builder().setCategory("type").setPrefix(true).build())
                )
            );
        assertSuggestions("foo", prefix, "suggestion0", "suggestion1", "suggestion2", "suggestion3", "suggestion4");
    }

    public void testGeoFiltering() throws Exception {
        LinkedHashMap<String, ContextMapping<?>> map = new LinkedHashMap<>();
        map.put("geo", ContextBuilder.geo("geo").build());
        final CompletionMappingBuilder mapping = new CompletionMappingBuilder().context(map);
        createIndexAndMapping(mapping);
        int numDocs = 10;
        List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        GeoPoint[] geoPoints = new GeoPoint[] { new GeoPoint("ezs42e44yx96"), new GeoPoint("u4pruydqqvj8") };
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder source = jsonBuilder().startObject()
                .startObject(FIELD)
                .field("input", "suggestion" + i)
                .field("weight", i + 1)
                .startObject("contexts")
                .field("geo", (i % 2 == 0) ? geoPoints[0].getGeohash() : geoPoints[1].getGeohash())
                .endObject()
                .endObject()
                .endObject();
            indexRequestBuilders.add(prepareIndex(INDEX).setId("" + i).setSource(source));
        }
        indexRandom(true, indexRequestBuilders);

        CompletionSuggestionBuilder geoFilteringPrefix = SuggestBuilders.completionSuggestion(FIELD)
            .prefix("sugg")
            .contexts(
                Collections.singletonMap(
                    "geo",
                    Collections.singletonList(GeoQueryContext.builder().setGeoPoint(new GeoPoint(geoPoints[0])).build())
                )
            );

        assertSuggestions("foo", geoFilteringPrefix, "suggestion8", "suggestion6", "suggestion4", "suggestion2", "suggestion0");
    }

    public void testGeoBoosting() throws Exception {
        LinkedHashMap<String, ContextMapping<?>> map = new LinkedHashMap<>();
        map.put("geo", ContextBuilder.geo("geo").build());
        final CompletionMappingBuilder mapping = new CompletionMappingBuilder().context(map);
        createIndexAndMapping(mapping);
        int numDocs = 10;
        List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        GeoPoint[] geoPoints = new GeoPoint[] { new GeoPoint("ezs42e44yx96"), new GeoPoint("u4pruydqqvj8") };
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder source = jsonBuilder().startObject()
                .startObject(FIELD)
                .field("input", "suggestion" + i)
                .field("weight", i + 1)
                .startObject("contexts")
                .field("geo", (i % 2 == 0) ? geoPoints[0].getGeohash() : geoPoints[1].getGeohash())
                .endObject()
                .endObject()
                .endObject();
            indexRequestBuilders.add(prepareIndex(INDEX).setId("" + i).setSource(source));
        }
        indexRandom(true, indexRequestBuilders);

        GeoQueryContext context1 = GeoQueryContext.builder().setGeoPoint(geoPoints[0]).setBoost(11).build();
        GeoQueryContext context2 = GeoQueryContext.builder().setGeoPoint(geoPoints[1]).build();
        CompletionSuggestionBuilder geoBoostingPrefix = SuggestBuilders.completionSuggestion(FIELD)
            .prefix("sugg")
            .contexts(Collections.singletonMap("geo", Arrays.asList(context1, context2)));

        assertSuggestions("foo", geoBoostingPrefix, "suggestion8", "suggestion6", "suggestion4", "suggestion2", "suggestion0");
    }

    public void testGeoPointContext() throws Exception {
        LinkedHashMap<String, ContextMapping<?>> map = new LinkedHashMap<>();
        map.put("geo", ContextBuilder.geo("geo").build());
        final CompletionMappingBuilder mapping = new CompletionMappingBuilder().context(map);
        createIndexAndMapping(mapping);
        int numDocs = 10;
        List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder source = jsonBuilder().startObject()
                .startObject(FIELD)
                .field("input", "suggestion" + i)
                .field("weight", i + 1)
                .startObject("contexts")
                .startObject("geo")
                .field("lat", 52.22)
                .field("lon", 4.53)
                .endObject()
                .endObject()
                .endObject()
                .endObject();
            indexRequestBuilders.add(prepareIndex(INDEX).setId("" + i).setSource(source));
        }
        indexRandom(true, indexRequestBuilders);
        CompletionSuggestionBuilder prefix = SuggestBuilders.completionSuggestion(FIELD)
            .prefix("sugg")
            .contexts(
                Collections.singletonMap(
                    "geo",
                    Collections.singletonList(GeoQueryContext.builder().setGeoPoint(new GeoPoint(52.2263, 4.543)).build())
                )
            );
        assertSuggestions("foo", prefix, "suggestion9", "suggestion8", "suggestion7", "suggestion6", "suggestion5");
    }

    public void testGeoNeighbours() throws Exception {
        String geohash = "gcpv";
        List<String> neighbours = new ArrayList<>();
        neighbours.add("gcpw");
        neighbours.add("gcpy");
        neighbours.add("u10n");
        neighbours.add("gcpt");
        neighbours.add("u10j");
        neighbours.add("gcps");
        neighbours.add("gcpu");
        neighbours.add("u10h");

        LinkedHashMap<String, ContextMapping<?>> map = new LinkedHashMap<>();
        map.put("geo", ContextBuilder.geo("geo").precision(4).build());
        final CompletionMappingBuilder mapping = new CompletionMappingBuilder().context(map);
        createIndexAndMapping(mapping);
        int numDocs = 10;
        List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            XContentBuilder source = jsonBuilder().startObject()
                .startObject(FIELD)
                .field("input", "suggestion" + i)
                .field("weight", i + 1)
                .startObject("contexts")
                .field("geo", randomFrom(neighbours))
                .endObject()
                .endObject()
                .endObject();
            indexRequestBuilders.add(prepareIndex(INDEX).setId("" + i).setSource(source));
        }
        indexRandom(true, indexRequestBuilders);

        CompletionSuggestionBuilder geoNeighbourPrefix = SuggestBuilders.completionSuggestion(FIELD)
            .prefix("sugg")
            .contexts(
                Collections.singletonMap(
                    "geo",
                    Collections.singletonList(GeoQueryContext.builder().setGeoPoint(GeoPoint.fromGeohash(geohash)).build())
                )
            );

        assertSuggestions("foo", geoNeighbourPrefix, "suggestion9", "suggestion8", "suggestion7", "suggestion6", "suggestion5");
    }

    public void testGeoField() throws Exception {
        XContentBuilder mapping = jsonBuilder();
        mapping.startObject();
        mapping.startObject("_doc");
        mapping.startObject("properties");
        mapping.startObject("location");
        mapping.startObject("properties");
        mapping.startObject("pin");
        mapping.field("type", "geo_point");
        // Enable store and disable indexing sometimes
        if (randomBoolean()) {
            mapping.field("store", "true");
        }
        if (randomBoolean()) {
            mapping.field("index", "false");
        }
        mapping.endObject(); // pin
        mapping.endObject();
        mapping.endObject(); // location
        mapping.startObject(FIELD);
        mapping.field("type", "completion");
        mapping.field("analyzer", "simple");

        mapping.startArray("contexts");
        mapping.startObject();
        mapping.field("name", "st");
        mapping.field("type", "geo");
        mapping.field("path", "location.pin");
        mapping.field("precision", 5);
        mapping.endObject();
        mapping.endArray();

        mapping.endObject();
        mapping.endObject();
        mapping.endObject();
        mapping.endObject();

        assertAcked(prepareCreate(INDEX).setMapping(mapping));

        XContentBuilder source1 = jsonBuilder().startObject()
            .startObject("location")
            .latlon("pin", 52.529172, 13.407333)
            .endObject()
            .startObject(FIELD)
            .array("input", "Hotel Amsterdam in Berlin")
            .endObject()
            .endObject();
        prepareIndex(INDEX).setId("1").setSource(source1).get();

        XContentBuilder source2 = jsonBuilder().startObject()
            .startObject("location")
            .latlon("pin", 52.363389, 4.888695)
            .endObject()
            .startObject(FIELD)
            .array("input", "Hotel Berlin in Amsterdam")
            .endObject()
            .endObject();
        prepareIndex(INDEX).setId("2").setSource(source2).get();

        refresh();

        String suggestionName = randomAlphaOfLength(10);
        CompletionSuggestionBuilder context = SuggestBuilders.completionSuggestion(FIELD)
            .text("h")
            .size(10)
            .contexts(
                Collections.singletonMap(
                    "st",
                    Collections.singletonList(GeoQueryContext.builder().setGeoPoint(new GeoPoint(52.52, 13.4)).build())
                )
            );
        assertResponse(prepareSearch(INDEX).suggest(new SuggestBuilder().addSuggestion(suggestionName, context)), response -> {
            assertEquals(response.getSuggest().size(), 1);
            assertEquals(
                "Hotel Amsterdam in Berlin",
                response.getSuggest().getSuggestion(suggestionName).iterator().next().getOptions().iterator().next().getText().string()
            );
        });
    }

    public void testSkipDuplicatesWithContexts() throws Exception {
        LinkedHashMap<String, ContextMapping<?>> map = new LinkedHashMap<>();
        map.put("type", ContextBuilder.category("type").field("type").build());
        map.put("cat", ContextBuilder.category("cat").field("cat").build());
        final CompletionMappingBuilder mapping = new CompletionMappingBuilder().context(map);
        createIndexAndMapping(mapping);
        int numDocs = randomIntBetween(10, 100);
        int numUnique = randomIntBetween(1, numDocs);
        List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            int id = i % numUnique;
            XContentBuilder source = jsonBuilder().startObject()
                .startObject(FIELD)
                .field("input", "suggestion" + id)
                .field("weight", id)
                .endObject()
                .field("cat", "cat" + id % 2)
                .field("type", "type" + id)
                .endObject();
            indexRequestBuilders.add(prepareIndex(INDEX).setId("" + i).setSource(source));
        }
        String[] expected = new String[numUnique];
        for (int i = 0; i < numUnique; i++) {
            expected[i] = "suggestion" + (numUnique - 1 - i);
        }
        indexRandom(true, indexRequestBuilders);
        Map<String, List<? extends ToXContent>> contextMap = new HashMap<>();
        contextMap.put("cat", Arrays.asList(CategoryQueryContext.builder().setCategory("cat0").build()));
        CompletionSuggestionBuilder completionSuggestionBuilder = SuggestBuilders.completionSuggestion(FIELD)
            .prefix("sugg")
            .contexts(contextMap)
            .skipDuplicates(true)
            .size(numUnique);

        String[] expectedModulo = Arrays.stream(expected)
            .filter((s) -> Integer.parseInt(s.substring("suggestion".length())) % 2 == 0)
            .toArray(String[]::new);
        assertSuggestions("suggestions", completionSuggestionBuilder, expectedModulo);
    }

    public void assertSuggestions(String suggestionName, SuggestionBuilder<?> suggestBuilder, String... suggestions) {
        assertResponse(
            prepareSearch(INDEX).suggest(new SuggestBuilder().addSuggestion(suggestionName, suggestBuilder)),
            response -> CompletionSuggestSearchIT.assertSuggestions(response, suggestionName, suggestions)
        );
    }

    private void createIndexAndMapping(CompletionMappingBuilder completionMappingBuilder) throws IOException {
        createIndexAndMappingAndSettings(Settings.EMPTY, completionMappingBuilder);
    }

    private void createIndexAndMappingAndSettings(Settings settings, CompletionMappingBuilder completionMappingBuilder) throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject(FIELD)
            .field("type", "completion")
            .field("analyzer", completionMappingBuilder.indexAnalyzer)
            .field("search_analyzer", completionMappingBuilder.searchAnalyzer)
            .field("preserve_separators", completionMappingBuilder.preserveSeparators)
            .field("preserve_position_increments", completionMappingBuilder.preservePositionIncrements);

        List<String> categoryContextFields = new ArrayList<>();
        if (completionMappingBuilder.contextMappings != null) {
            mapping.startArray("contexts");
            for (Map.Entry<String, ContextMapping<?>> contextMapping : completionMappingBuilder.contextMappings.entrySet()) {
                mapping.startObject()
                    .field("name", contextMapping.getValue().name())
                    .field("type", contextMapping.getValue().type().name());
                switch (contextMapping.getValue().type()) {
                    case CATEGORY -> {
                        final String fieldName = ((CategoryContextMapping) contextMapping.getValue()).getFieldName();
                        if (fieldName != null) {
                            mapping.field("path", fieldName);
                            categoryContextFields.add(fieldName);
                        }
                    }
                    case GEO -> {
                        final String name = ((GeoContextMapping) contextMapping.getValue()).getFieldName();
                        mapping.field("precision", ((GeoContextMapping) contextMapping.getValue()).getPrecision());
                        if (name != null) {
                            mapping.field("path", name);
                        }
                    }
                }

                mapping.endObject();
            }

            mapping.endArray();
        }
        mapping.endObject();
        for (String fieldName : categoryContextFields) {
            mapping.startObject(fieldName).field("type", randomBoolean() ? "keyword" : "text").endObject();
        }
        mapping.endObject().endObject().endObject();

        assertAcked(
            indicesAdmin().prepareCreate(INDEX).setSettings(Settings.builder().put(indexSettings()).put(settings)).setMapping(mapping)
        );
    }
}
