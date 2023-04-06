/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;

public class SortBuilderTests extends ESTestCase {
    private static final int NUMBER_OF_RUNS = 20;

    private static NamedXContentRegistry xContentRegistry;

    @BeforeClass
    public static void init() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        xContentRegistry = null;
    }

    /**
     * test two syntax variations:
     * - "sort" : "fieldname"
     * - "sort" : { "fieldname" : "asc" }
     */
    public void testSingleFieldSort() throws IOException {
        SortOrder order = randomBoolean() ? SortOrder.ASC : SortOrder.DESC;
        String json = Strings.format("{ \"sort\" : { \"field1\" : \"%s\" }}", order);
        List<SortBuilder<?>> result = parseSort(json);
        assertEquals(1, result.size());
        SortBuilder<?> sortBuilder = result.get(0);
        assertEquals(new FieldSortBuilder("field1").order(order), sortBuilder);

        json = "{ \"sort\" : \"field1\" }";
        result = parseSort(json);
        assertEquals(1, result.size());
        sortBuilder = result.get(0);
        assertEquals(new FieldSortBuilder("field1"), sortBuilder);

        // one element array, see https://github.com/elastic/elasticsearch/issues/17257
        json = "{ \"sort\" : [\"field1\"] }";
        result = parseSort(json);
        assertEquals(1, result.size());
        sortBuilder = result.get(0);
        assertEquals(new FieldSortBuilder("field1"), sortBuilder);

        json = "{ \"sort\" : { \"_doc\" : \"" + order + "\" }}";
        result = parseSort(json);
        assertEquals(1, result.size());
        sortBuilder = result.get(0);
        assertEquals(new FieldSortBuilder("_doc").order(order), sortBuilder);

        json = "{ \"sort\" : \"_doc\" }";
        result = parseSort(json);
        assertEquals(1, result.size());
        sortBuilder = result.get(0);
        assertEquals(new FieldSortBuilder("_doc"), sortBuilder);

        json = "{ \"sort\" : { \"_score\" : \"" + order + "\" }}";
        result = parseSort(json);
        assertEquals(1, result.size());
        sortBuilder = result.get(0);
        assertEquals(new ScoreSortBuilder().order(order), sortBuilder);

        json = "{ \"sort\" : \"_score\" }";
        result = parseSort(json);
        assertEquals(1, result.size());
        sortBuilder = result.get(0);
        assertEquals(new ScoreSortBuilder(), sortBuilder);

        // test two spellings for _geo_disctance
        json = """
            { "sort" : [{"_geoDistance" : {"pin.location" : "40,-70" } }] }""";
        result = parseSort(json);
        assertEquals(1, result.size());
        sortBuilder = result.get(0);
        assertEquals(new GeoDistanceSortBuilder("pin.location", 40, -70), sortBuilder);

        json = """
            { "sort" : [{"_geo_distance" : {"pin.location" : "POINT (-70 40)" } }] }""";
        result = parseSort(json);
        assertEquals(1, result.size());
        sortBuilder = result.get(0);
        assertEquals(new GeoDistanceSortBuilder("pin.location", 40, -70), sortBuilder);
    }

    /**
     * test parsing random syntax variations
     */
    public void testRandomSortBuilders() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_RUNS; runs++) {
            Set<String> expectedWarningHeaders = new HashSet<>();
            List<SortBuilder<?>> testBuilders = randomSortBuilderList();
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
            xContentBuilder.startObject();
            if (testBuilders.size() > 1) {
                xContentBuilder.startArray("sort");
            } else {
                xContentBuilder.field("sort");
            }
            for (SortBuilder<?> builder : testBuilders) {
                if (builder instanceof ScoreSortBuilder || builder instanceof FieldSortBuilder) {
                    switch (randomIntBetween(0, 2)) {
                        case 0:
                            if (builder instanceof ScoreSortBuilder) {
                                xContentBuilder.value("_score");
                            } else {
                                xContentBuilder.value(((FieldSortBuilder) builder).getFieldName());
                            }
                            break;
                        case 1:
                            xContentBuilder.startObject();
                            if (builder instanceof ScoreSortBuilder) {
                                xContentBuilder.field("_score");
                            } else {
                                xContentBuilder.field(((FieldSortBuilder) builder).getFieldName());
                            }
                            xContentBuilder.value(builder.order());
                            xContentBuilder.endObject();
                            break;
                        case 2:
                            builder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
                            break;
                    }
                } else {
                    builder.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
                }
            }
            if (testBuilders.size() > 1) {
                xContentBuilder.endArray();
            }
            xContentBuilder.endObject();
            List<SortBuilder<?>> parsedSort = parseSort(Strings.toString(xContentBuilder));
            assertEquals(testBuilders.size(), parsedSort.size());
            Iterator<SortBuilder<?>> iterator = testBuilders.iterator();
            for (SortBuilder<?> parsedBuilder : parsedSort) {
                assertEquals(iterator.next(), parsedBuilder);
            }
            if (expectedWarningHeaders.size() > 0) {
                assertWarnings(expectedWarningHeaders.toArray(new String[expectedWarningHeaders.size()]));
            }
        }
    }

    public static List<SortBuilder<?>> randomSortBuilderList() {
        int size = randomIntBetween(1, 5);
        List<SortBuilder<?>> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(switch (randomIntBetween(0, 5)) {
                case 0 -> new ScoreSortBuilder();
                case 1 -> new FieldSortBuilder(randomAlphaOfLengthBetween(1, 10));
                case 2 -> SortBuilders.fieldSort(FieldSortBuilder.DOC_FIELD_NAME);
                case 3 -> GeoDistanceSortBuilderTests.randomGeoDistanceSortBuilder();
                case 4 -> ScriptSortBuilderTests.randomScriptSortBuilder();
                case 5 -> SortBuilders.pitTiebreaker();
                default -> throw new IllegalStateException("unexpected randomization in tests");
            });
        }
        return list;
    }

    /**
     * test array syntax variations:
     * - "sort" : [ "fieldname", { "fieldname2" : "asc" }, ...]
     */
    public void testMultiFieldSort() throws IOException {
        String json = """
            {
              "sort": [
                {
                  "post_date": {
                    "order": "asc"
                  }
                },
                "user",
                {
                  "name": "desc"
                },
                {
                  "age": "desc"
                },
                {
                  "_geo_distance": {
                    "pin.location": "POINT (-70 40)"
                  }
                },
                "_score"
              ]
            }""";
        List<SortBuilder<?>> result = parseSort(json);
        assertEquals(6, result.size());
        assertEquals(new FieldSortBuilder("post_date").order(SortOrder.ASC), result.get(0));
        assertEquals(new FieldSortBuilder("user").order(SortOrder.ASC), result.get(1));
        assertEquals(new FieldSortBuilder("name").order(SortOrder.DESC), result.get(2));
        assertEquals(new FieldSortBuilder("age").order(SortOrder.DESC), result.get(3));
        assertEquals(new GeoDistanceSortBuilder("pin.location", new GeoPoint(40, -70)), result.get(4));
        assertEquals(new ScoreSortBuilder(), result.get(5));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    private List<SortBuilder<?>> parseSort(String jsonString) throws IOException {
        try (XContentParser itemParser = createParser(JsonXContent.jsonXContent, jsonString)) {

            assertEquals(XContentParser.Token.START_OBJECT, itemParser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, itemParser.nextToken());
            assertEquals("sort", itemParser.currentName());
            itemParser.nextToken();
            return SortBuilder.fromXContent(itemParser);
        }
    }
}
