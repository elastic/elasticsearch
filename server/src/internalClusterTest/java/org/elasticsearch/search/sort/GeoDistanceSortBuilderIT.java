/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.GeoValidationMethod;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponses;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSortValues;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.closeTo;

public class GeoDistanceSortBuilderIT extends ESIntegTestCase {

    private static final String LOCATION_FIELD = "location";

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testManyToManyGeoPoints() throws ExecutionException, InterruptedException, IOException {
        /**
         * | q  |  d1    |   d2
         * |    |        |
         * |    |        |
         * |    |        |
         * |2  o|  x     |     x
         * |    |        |
         * |1  o|      x | x
         * |___________________________
         * 1   2   3   4   5   6   7
         */
        IndexVersion version = randomBoolean() ? IndexVersion.current() : IndexVersionUtils.randomCompatibleWriteVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        assertAcked(prepareCreate("index").setSettings(settings).setMapping(LOCATION_FIELD, "type=geo_point"));
        XContentBuilder d1Builder = jsonBuilder();
        GeoPoint[] d1Points = { new GeoPoint(3, 2), new GeoPoint(4, 1) };
        createShuffeldJSONArray(d1Builder, d1Points);

        XContentBuilder d2Builder = jsonBuilder();
        GeoPoint[] d2Points = { new GeoPoint(5, 1), new GeoPoint(6, 2) };
        createShuffeldJSONArray(d2Builder, d2Points);

        logger.info("d1: {}", d1Builder);
        logger.info("d2: {}", d2Builder);
        indexRandom(true, prepareIndex("index").setId("d1").setSource(d1Builder), prepareIndex("index").setId("d2").setSource(d2Builder));
        GeoPoint[] q = new GeoPoint[2];
        if (randomBoolean()) {
            q[0] = new GeoPoint(2, 1);
            q[1] = new GeoPoint(2, 2);
        } else {
            q[1] = new GeoPoint(2, 2);
            q[0] = new GeoPoint(2, 1);
        }

        assertResponse(
            prepareSearch().setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder(LOCATION_FIELD, q).sortMode(SortMode.MIN).order(SortOrder.ASC)),
            response -> {
                assertOrderedSearchHits(response, "d1", "d2");
                assertThat(
                    (Double) response.getHits().getAt(0).getSortValues()[0],
                    closeTo(GeoDistance.ARC.calculate(2, 2, 3, 2, DistanceUnit.METERS), 10d)
                );
                assertThat(
                    (Double) response.getHits().getAt(1).getSortValues()[0],
                    closeTo(GeoDistance.ARC.calculate(2, 1, 5, 1, DistanceUnit.METERS), 10d)
                );
            }
        );
        assertResponse(
            prepareSearch().setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder(LOCATION_FIELD, q).sortMode(SortMode.MIN).order(SortOrder.DESC)),
            response -> {
                assertOrderedSearchHits(response, "d2", "d1");
                assertThat(
                    (Double) response.getHits().getAt(0).getSortValues()[0],
                    closeTo(GeoDistance.ARC.calculate(2, 1, 5, 1, DistanceUnit.METERS), 10d)
                );
                assertThat(
                    (Double) response.getHits().getAt(1).getSortValues()[0],
                    closeTo(GeoDistance.ARC.calculate(2, 2, 3, 2, DistanceUnit.METERS), 10d)
                );
            }
        );
        assertResponse(
            prepareSearch().setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder(LOCATION_FIELD, q).sortMode(SortMode.MAX).order(SortOrder.ASC)),
            response -> {
                assertOrderedSearchHits(response, "d1", "d2");
                assertThat(
                    (Double) response.getHits().getAt(0).getSortValues()[0],
                    closeTo(GeoDistance.ARC.calculate(2, 2, 4, 1, DistanceUnit.METERS), 10d)
                );
                assertThat(
                    (Double) response.getHits().getAt(1).getSortValues()[0],
                    closeTo(GeoDistance.ARC.calculate(2, 1, 6, 2, DistanceUnit.METERS), 10d)
                );
            }
        );
        assertResponse(
            prepareSearch().setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder(LOCATION_FIELD, q).sortMode(SortMode.MAX).order(SortOrder.DESC)),
            response -> {
                assertOrderedSearchHits(response, "d2", "d1");
                assertThat(
                    (Double) response.getHits().getAt(0).getSortValues()[0],
                    closeTo(GeoDistance.ARC.calculate(2, 1, 6, 2, DistanceUnit.METERS), 10d)
                );
                assertThat(
                    (Double) response.getHits().getAt(1).getSortValues()[0],
                    closeTo(GeoDistance.ARC.calculate(2, 2, 4, 1, DistanceUnit.METERS), 10d)
                );
            }
        );
    }

    public void testSingeToManyAvgMedian() throws ExecutionException, InterruptedException, IOException {
        /**
         * q  = (0, 0)
         *
         * d1 = (0, 1), (0, 4), (0, 10); so avg. distance is 5, median distance is 4
         * d2 = (0, 1), (0, 5), (0, 6); so avg. distance is 4, median distance is 5
         */
        IndexVersion version = randomBoolean() ? IndexVersion.current() : IndexVersionUtils.randomCompatibleWriteVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        assertAcked(prepareCreate("index").setSettings(settings).setMapping(LOCATION_FIELD, "type=geo_point"));
        XContentBuilder d1Builder = jsonBuilder();
        GeoPoint[] d1Points = { new GeoPoint(0, 1), new GeoPoint(0, 4), new GeoPoint(0, 10) };
        createShuffeldJSONArray(d1Builder, d1Points);

        XContentBuilder d2Builder = jsonBuilder();
        GeoPoint[] d2Points = { new GeoPoint(0, 1), new GeoPoint(0, 5), new GeoPoint(0, 6) };
        createShuffeldJSONArray(d2Builder, d2Points);

        logger.info("d1: {}", d1Builder);
        logger.info("d2: {}", d2Builder);
        indexRandom(true, prepareIndex("index").setId("d1").setSource(d1Builder), prepareIndex("index").setId("d2").setSource(d2Builder));
        GeoPoint q = new GeoPoint(0, 0);

        assertResponse(
            prepareSearch().setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder(LOCATION_FIELD, q).sortMode(SortMode.AVG).order(SortOrder.ASC)),
            response -> {
                assertOrderedSearchHits(response, "d2", "d1");
                assertThat(
                    (Double) response.getHits().getAt(0).getSortValues()[0],
                    closeTo(GeoDistance.ARC.calculate(0, 0, 0, 4, DistanceUnit.METERS), 10d)
                );
                assertThat(
                    (Double) response.getHits().getAt(1).getSortValues()[0],
                    closeTo(GeoDistance.ARC.calculate(0, 0, 0, 5, DistanceUnit.METERS), 10d)
                );
            }
        );
        assertResponse(
            prepareSearch().setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder(LOCATION_FIELD, q).sortMode(SortMode.MEDIAN).order(SortOrder.ASC)),
            response -> {
                assertOrderedSearchHits(response, "d1", "d2");
                assertThat(
                    (Double) response.getHits().getAt(0).getSortValues()[0],
                    closeTo(GeoDistance.ARC.calculate(0, 0, 0, 4, DistanceUnit.METERS), 10d)
                );
                assertThat(
                    (Double) response.getHits().getAt(1).getSortValues()[0],
                    closeTo(GeoDistance.ARC.calculate(0, 0, 0, 5, DistanceUnit.METERS), 10d)
                );
            }
        );
    }

    protected void createShuffeldJSONArray(XContentBuilder builder, GeoPoint[] pointsArray) throws IOException {
        List<GeoPoint> points = new ArrayList<>();
        points.addAll(Arrays.asList(pointsArray));
        builder.startObject();
        builder.startArray(LOCATION_FIELD);
        int numPoints = points.size();
        for (int i = 0; i < numPoints; i++) {
            builder.value(points.remove(randomInt(points.size() - 1)));
        }
        builder.endArray();
        builder.endObject();
    }

    public void testManyToManyGeoPointsWithDifferentFormats() throws ExecutionException, InterruptedException, IOException {
        /**   q     d1       d2
         * |4  o|   x    |   x
         * |    |        |
         * |3  o|  x     |  x
         * |    |        |
         * |2  o| x      | x
         * |    |        |
         * |1  o|x       |x
         * |______________________
         * 1   2   3   4   5   6
         */
        IndexVersion version = randomBoolean() ? IndexVersion.current() : IndexVersionUtils.randomCompatibleWriteVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        assertAcked(prepareCreate("index").setSettings(settings).setMapping(LOCATION_FIELD, "type=geo_point"));
        XContentBuilder d1Builder = jsonBuilder();
        GeoPoint[] d1Points = { new GeoPoint(2.5, 1), new GeoPoint(2.75, 2), new GeoPoint(3, 3), new GeoPoint(3.25, 4) };
        createShuffeldJSONArray(d1Builder, d1Points);

        XContentBuilder d2Builder = jsonBuilder();
        GeoPoint[] d2Points = { new GeoPoint(4.5, 1), new GeoPoint(4.75, 2), new GeoPoint(5, 3), new GeoPoint(5.25, 4) };
        createShuffeldJSONArray(d2Builder, d2Points);

        indexRandom(true, prepareIndex("index").setId("d1").setSource(d1Builder), prepareIndex("index").setId("d2").setSource(d2Builder));

        List<GeoPoint> qPoints = Arrays.asList(new GeoPoint(2, 1), new GeoPoint(2, 2), new GeoPoint(2, 3), new GeoPoint(2, 4));
        Collections.shuffle(qPoints, random());

        GeoDistanceSortBuilder geoDistanceSortBuilder = null;
        for (GeoPoint point : qPoints) {
            if (geoDistanceSortBuilder == null) {
                geoDistanceSortBuilder = new GeoDistanceSortBuilder(LOCATION_FIELD, point);
            } else {
                geoDistanceSortBuilder.points(point);
            }
        }

        assertResponse(
            prepareSearch().setQuery(matchAllQuery()).addSort(geoDistanceSortBuilder.sortMode(SortMode.MIN).order(SortOrder.ASC)),
            response -> {
                assertOrderedSearchHits(response, "d1", "d2");
                assertThat(
                    (Double) response.getHits().getAt(0).getSortValues()[0],
                    closeTo(GeoDistance.ARC.calculate(2.5, 1, 2, 1, DistanceUnit.METERS), 1.e-1)
                );
                assertThat(
                    (Double) response.getHits().getAt(1).getSortValues()[0],
                    closeTo(GeoDistance.ARC.calculate(4.5, 1, 2, 1, DistanceUnit.METERS), 1.e-1)
                );
            }
        );
        assertResponse(
            prepareSearch().setQuery(matchAllQuery()).addSort(geoDistanceSortBuilder.sortMode(SortMode.MAX).order(SortOrder.ASC)),
            response -> {
                assertOrderedSearchHits(response, "d1", "d2");
                assertThat(
                    (Double) response.getHits().getAt(0).getSortValues()[0],
                    closeTo(GeoDistance.ARC.calculate(3.25, 4, 2, 1, DistanceUnit.METERS), 1.e-1)
                );
                assertThat(
                    (Double) response.getHits().getAt(1).getSortValues()[0],
                    closeTo(GeoDistance.ARC.calculate(5.25, 4, 2, 1, DistanceUnit.METERS), 1.e-1)
                );
            }
        );

    }

    public void testSinglePointGeoDistanceSort() throws ExecutionException, InterruptedException, IOException {
        assertAcked(prepareCreate("index").setMapping(LOCATION_FIELD, "type=geo_point"));
        indexRandom(
            true,
            prepareIndex("index").setId("d1")
                .setSource(jsonBuilder().startObject().startObject(LOCATION_FIELD).field("lat", 1).field("lon", 1).endObject().endObject()),
            prepareIndex("index").setId("d2")
                .setSource(jsonBuilder().startObject().startObject(LOCATION_FIELD).field("lat", 1).field("lon", 2).endObject().endObject())
        );

        String hashPoint = "s037ms06g7h0";

        assertResponses(
            response -> checkCorrectSortOrderForGeoSort(response),
            prepareSearch().setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder(LOCATION_FIELD, hashPoint).sortMode(SortMode.MIN).order(SortOrder.ASC)),
            prepareSearch().setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder(LOCATION_FIELD, new GeoPoint(2, 2)).sortMode(SortMode.MIN).order(SortOrder.ASC)),
            prepareSearch().setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder(LOCATION_FIELD, 2, 2).sortMode(SortMode.MIN).order(SortOrder.ASC)),
            prepareSearch().setSource(new SearchSourceBuilder().sort(SortBuilders.geoDistanceSort(LOCATION_FIELD, 2.0, 2.0))),
            prepareSearch().setSource(new SearchSourceBuilder().sort(SortBuilders.geoDistanceSort(LOCATION_FIELD, "s037ms06g7h0"))),
            prepareSearch().setSource(new SearchSourceBuilder().sort(SortBuilders.geoDistanceSort(LOCATION_FIELD, 2.0, 2.0))),
            prepareSearch().setSource(
                new SearchSourceBuilder().sort(
                    SortBuilders.geoDistanceSort(LOCATION_FIELD, 2.0, 2.0).validation(GeoValidationMethod.COERCE)
                )
            )
        );
    }

    private static void checkCorrectSortOrderForGeoSort(SearchResponse searchResponse) {
        assertOrderedSearchHits(searchResponse, "d2", "d1");
        assertThat(
            (Double) searchResponse.getHits().getAt(0).getSortValues()[0],
            closeTo(GeoDistance.ARC.calculate(2, 2, 1, 2, DistanceUnit.METERS), 1.e-1)
        );
        assertThat(
            (Double) searchResponse.getHits().getAt(1).getSortValues()[0],
            closeTo(GeoDistance.ARC.calculate(2, 2, 1, 1, DistanceUnit.METERS), 1.e-1)
        );
    }

    public void testCrossIndexIgnoreUnmapped() throws Exception {
        assertAcked(
            prepareCreate("test1").setMapping("str_field", "type=keyword", "long_field", "type=long", "double_field", "type=double")
        );
        assertAcked(prepareCreate("test2"));

        indexRandom(
            true,
            prepareIndex("test1").setSource("str_field", "bcd", "long_field", 3, "double_field", 0.65),
            prepareIndex("test2").setSource()
        );

        assertSortValues(
            prepareSearch("test1", "test2").addSort(fieldSort("str_field").order(SortOrder.ASC).unmappedType("keyword"))
                .addSort(fieldSort("str_field2").order(SortOrder.DESC).unmappedType("keyword")),
            new Object[] { "bcd", null },
            new Object[] { null, null }
        );

        assertSortValues(
            prepareSearch("test1", "test2").addSort(fieldSort("long_field").order(SortOrder.ASC).unmappedType("long"))
                .addSort(fieldSort("long_field2").order(SortOrder.DESC).unmappedType("long")),
            new Object[] { 3L, Long.MIN_VALUE },
            new Object[] { Long.MAX_VALUE, Long.MIN_VALUE }
        );

        assertSortValues(
            prepareSearch("test1", "test2").addSort(fieldSort("double_field").order(SortOrder.ASC).unmappedType("double"))
                .addSort(fieldSort("double_field2").order(SortOrder.DESC).unmappedType("double")),
            new Object[] { 0.65, Double.NEGATIVE_INFINITY },
            new Object[] { Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY }
        );
    }
}
