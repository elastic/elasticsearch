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

package org.elasticsearch.search.sort;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.GeoValidationMethod;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSortValues;
import static org.hamcrest.Matchers.closeTo;

public class GeoDistanceSortBuilderIT extends ESIntegTestCase {
    private static final String LOCATION_FIELD = "location";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class);
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
        Version version = randomBoolean() ? Version.CURRENT
                : VersionUtils.randomVersionBetween(random(), Version.V_5_0_0, Version.CURRENT);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        assertAcked(prepareCreate("index").setSettings(settings).addMapping("type", LOCATION_FIELD, "type=geo_point"));
        XContentBuilder d1Builder = jsonBuilder();
        GeoPoint[] d1Points = {new GeoPoint(3, 2), new GeoPoint(4, 1)};
        createShuffeldJSONArray(d1Builder, d1Points);

        XContentBuilder d2Builder = jsonBuilder();
        GeoPoint[] d2Points = {new GeoPoint(5, 1), new GeoPoint(6, 2)};
        createShuffeldJSONArray(d2Builder, d2Points);

        logger.info("d1: {}", d1Builder);
        logger.info("d2: {}", d2Builder);
        indexRandom(true,
                client().prepareIndex("index", "type", "d1").setSource(d1Builder),
                client().prepareIndex("index", "type", "d2").setSource(d2Builder));
        GeoPoint[] q = new GeoPoint[2];
        if (randomBoolean()) {
            q[0] = new GeoPoint(2, 1);
            q[1] = new GeoPoint(2, 2);
        } else {
            q[1] = new GeoPoint(2, 2);
            q[0] = new GeoPoint(2, 1);
        }

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder(LOCATION_FIELD, q).sortMode(SortMode.MIN).order(SortOrder.ASC))
                .execute().actionGet();
        assertOrderedSearchHits(searchResponse, "d1", "d2");
        assertThat((Double)searchResponse.getHits().getAt(0).getSortValues()[0], closeTo(GeoDistance.ARC.calculate(2, 2, 3, 2, DistanceUnit.METERS), 10d));
        assertThat((Double)searchResponse.getHits().getAt(1).getSortValues()[0], closeTo(GeoDistance.ARC.calculate(2, 1, 5, 1, DistanceUnit.METERS), 10d));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder(LOCATION_FIELD, q).sortMode(SortMode.MIN).order(SortOrder.DESC))
                .execute().actionGet();
        assertOrderedSearchHits(searchResponse, "d2", "d1");
        assertThat((Double)searchResponse.getHits().getAt(0).getSortValues()[0], closeTo(GeoDistance.ARC.calculate(2, 1, 5, 1, DistanceUnit.METERS), 10d));
        assertThat((Double)searchResponse.getHits().getAt(1).getSortValues()[0], closeTo(GeoDistance.ARC.calculate(2, 2, 3, 2, DistanceUnit.METERS), 10d));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder(LOCATION_FIELD, q).sortMode(SortMode.MAX).order(SortOrder.ASC))
                .execute().actionGet();
        assertOrderedSearchHits(searchResponse, "d1", "d2");
        assertThat((Double)searchResponse.getHits().getAt(0).getSortValues()[0], closeTo(GeoDistance.ARC.calculate(2, 2, 4, 1, DistanceUnit.METERS), 10d));
        assertThat((Double)searchResponse.getHits().getAt(1).getSortValues()[0], closeTo(GeoDistance.ARC.calculate(2, 1, 6, 2, DistanceUnit.METERS), 10d));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder(LOCATION_FIELD, q).sortMode(SortMode.MAX).order(SortOrder.DESC))
                .execute().actionGet();
        assertOrderedSearchHits(searchResponse, "d2", "d1");
        assertThat((Double)searchResponse.getHits().getAt(0).getSortValues()[0], closeTo(GeoDistance.ARC.calculate(2, 1, 6, 2, DistanceUnit.METERS), 10d));
        assertThat((Double)searchResponse.getHits().getAt(1).getSortValues()[0], closeTo(GeoDistance.ARC.calculate(2, 2, 4, 1, DistanceUnit.METERS), 10d));
    }

    public void testSingeToManyAvgMedian() throws ExecutionException, InterruptedException, IOException {
        /**
         * q  = (0, 0)
         *
         * d1 = (0, 1), (0, 4), (0, 10); so avg. distance is 5, median distance is 4
         * d2 = (0, 1), (0, 5), (0, 6); so avg. distance is 4, median distance is 5
         */
        Version version = randomBoolean() ? Version.CURRENT
                : VersionUtils.randomVersionBetween(random(), Version.V_5_0_0, Version.CURRENT);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        assertAcked(prepareCreate("index").setSettings(settings).addMapping("type", LOCATION_FIELD, "type=geo_point"));
        XContentBuilder d1Builder = jsonBuilder();
        GeoPoint[] d1Points = {new GeoPoint(0, 1), new GeoPoint(0, 4), new GeoPoint(0, 10)};
        createShuffeldJSONArray(d1Builder, d1Points);

        XContentBuilder d2Builder = jsonBuilder();
        GeoPoint[] d2Points = {new GeoPoint(0, 1), new GeoPoint(0, 5), new GeoPoint(0, 6)};
        createShuffeldJSONArray(d2Builder, d2Points);

        logger.info("d1: {}", d1Builder);
        logger.info("d2: {}", d2Builder);
        indexRandom(true,
                client().prepareIndex("index", "type", "d1").setSource(d1Builder),
                client().prepareIndex("index", "type", "d2").setSource(d2Builder));
        GeoPoint q = new GeoPoint(0,0);

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder(LOCATION_FIELD, q).sortMode(SortMode.AVG).order(SortOrder.ASC))
                .execute().actionGet();
        assertOrderedSearchHits(searchResponse, "d2", "d1");
        assertThat((Double)searchResponse.getHits().getAt(0).getSortValues()[0], closeTo(GeoDistance.ARC.calculate(0, 0, 0, 4, DistanceUnit.METERS), 10d));
        assertThat((Double)searchResponse.getHits().getAt(1).getSortValues()[0], closeTo(GeoDistance.ARC.calculate(0, 0, 0, 5, DistanceUnit.METERS), 10d));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(new GeoDistanceSortBuilder(LOCATION_FIELD, q).sortMode(SortMode.MEDIAN).order(SortOrder.ASC))
                .execute().actionGet();
        assertOrderedSearchHits(searchResponse, "d1", "d2");
        assertThat((Double)searchResponse.getHits().getAt(0).getSortValues()[0], closeTo(GeoDistance.ARC.calculate(0, 0, 0, 4, DistanceUnit.METERS), 10d));
        assertThat((Double)searchResponse.getHits().getAt(1).getSortValues()[0], closeTo(GeoDistance.ARC.calculate(0, 0, 0, 5, DistanceUnit.METERS), 10d));
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
        Version version = randomBoolean() ? Version.CURRENT
                : VersionUtils.randomVersionBetween(random(), Version.V_5_0_0, Version.CURRENT);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        assertAcked(prepareCreate("index").setSettings(settings).addMapping("type", LOCATION_FIELD, "type=geo_point"));
        XContentBuilder d1Builder = jsonBuilder();
        GeoPoint[] d1Points = {new GeoPoint(2.5, 1), new GeoPoint(2.75, 2), new GeoPoint(3, 3), new GeoPoint(3.25, 4)};
        createShuffeldJSONArray(d1Builder, d1Points);

        XContentBuilder d2Builder = jsonBuilder();
        GeoPoint[] d2Points = {new GeoPoint(4.5, 1), new GeoPoint(4.75, 2), new GeoPoint(5, 3), new GeoPoint(5.25, 4)};
        createShuffeldJSONArray(d2Builder, d2Points);

        indexRandom(true,
                client().prepareIndex("index", "type", "d1").setSource(d1Builder),
                client().prepareIndex("index", "type", "d2").setSource(d2Builder));

        List<String> qHashes = new ArrayList<>();
        List<GeoPoint> qPoints = new ArrayList<>();
        createQPoints(qHashes, qPoints);

        GeoDistanceSortBuilder geoDistanceSortBuilder = null;
        for (int i = 0; i < 4; i++) {
            int at = randomInt(3 - i);
            if (randomBoolean()) {
                if (geoDistanceSortBuilder == null) {
                    geoDistanceSortBuilder = new GeoDistanceSortBuilder(LOCATION_FIELD, qHashes.get(at));
                } else {
                    geoDistanceSortBuilder.geohashes(qHashes.get(at));
                }
            } else {
                if (geoDistanceSortBuilder == null) {
                    geoDistanceSortBuilder = new GeoDistanceSortBuilder(LOCATION_FIELD, qPoints.get(at));
                } else {
                    geoDistanceSortBuilder.points(qPoints.get(at));
                }
            }
            qHashes.remove(at);
            qPoints.remove(at);
        }

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(geoDistanceSortBuilder.sortMode(SortMode.MIN).order(SortOrder.ASC))
                .execute().actionGet();
        assertOrderedSearchHits(searchResponse, "d1", "d2");
        assertThat((Double) searchResponse.getHits().getAt(0).getSortValues()[0], closeTo(GeoDistance.ARC.calculate(2.5, 1, 2, 1, DistanceUnit.METERS), 1.e-1));
        assertThat((Double) searchResponse.getHits().getAt(1).getSortValues()[0], closeTo(GeoDistance.ARC.calculate(4.5, 1, 2, 1, DistanceUnit.METERS), 1.e-1));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(geoDistanceSortBuilder.sortMode(SortMode.MAX).order(SortOrder.ASC))
                .execute().actionGet();
        assertOrderedSearchHits(searchResponse, "d1", "d2");
        assertThat((Double) searchResponse.getHits().getAt(0).getSortValues()[0], closeTo(GeoDistance.ARC.calculate(3.25, 4, 2, 1, DistanceUnit.METERS), 1.e-1));
        assertThat((Double) searchResponse.getHits().getAt(1).getSortValues()[0], closeTo(GeoDistance.ARC.calculate(5.25, 4, 2, 1, DistanceUnit.METERS), 1.e-1));

    }

    public void testSinglePointGeoDistanceSort() throws ExecutionException, InterruptedException, IOException {
        assertAcked(prepareCreate("index").addMapping("type", LOCATION_FIELD, "type=geo_point"));
        indexRandom(true,
                client().prepareIndex("index", "type", "d1").setSource(jsonBuilder().startObject().startObject(LOCATION_FIELD).field("lat", 1).field("lon", 1).endObject().endObject()),
                client().prepareIndex("index", "type", "d2").setSource(jsonBuilder().startObject().startObject(LOCATION_FIELD).field("lat", 1).field("lon", 2).endObject().endObject()));

        String hashPoint = "s037ms06g7h0";

        GeoDistanceSortBuilder geoDistanceSortBuilder = new GeoDistanceSortBuilder(LOCATION_FIELD, hashPoint);

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(geoDistanceSortBuilder.sortMode(SortMode.MIN).order(SortOrder.ASC))
                .execute().actionGet();
        checkCorrectSortOrderForGeoSort(searchResponse);

        geoDistanceSortBuilder = new GeoDistanceSortBuilder(LOCATION_FIELD, new GeoPoint(2, 2));

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(geoDistanceSortBuilder.sortMode(SortMode.MIN).order(SortOrder.ASC))
                .execute().actionGet();
        checkCorrectSortOrderForGeoSort(searchResponse);

        geoDistanceSortBuilder = new GeoDistanceSortBuilder(LOCATION_FIELD, 2, 2);

        searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .addSort(geoDistanceSortBuilder.sortMode(SortMode.MIN).order(SortOrder.ASC))
                .execute().actionGet();
        checkCorrectSortOrderForGeoSort(searchResponse);

        searchResponse = client()
                .prepareSearch()
                .setSource(
                        new SearchSourceBuilder().sort(SortBuilders.geoDistanceSort(LOCATION_FIELD, 2.0, 2.0)
                                )).execute().actionGet();
        checkCorrectSortOrderForGeoSort(searchResponse);

        searchResponse = client()
                .prepareSearch()
                .setSource(
                        new SearchSourceBuilder().sort(SortBuilders.geoDistanceSort(LOCATION_FIELD, "s037ms06g7h0")
                                )).execute().actionGet();
        checkCorrectSortOrderForGeoSort(searchResponse);

        searchResponse = client()
                .prepareSearch()
                .setSource(
                        new SearchSourceBuilder().sort(SortBuilders.geoDistanceSort(LOCATION_FIELD, 2.0, 2.0)
                                )).execute().actionGet();
        checkCorrectSortOrderForGeoSort(searchResponse);

        searchResponse = client()
                .prepareSearch()
                .setSource(
                        new SearchSourceBuilder().sort(SortBuilders.geoDistanceSort(LOCATION_FIELD, 2.0, 2.0)
                                .validation(GeoValidationMethod.COERCE))).execute().actionGet();
        checkCorrectSortOrderForGeoSort(searchResponse);
    }

    private static void checkCorrectSortOrderForGeoSort(SearchResponse searchResponse) {
        assertOrderedSearchHits(searchResponse, "d2", "d1");
        assertThat((Double) searchResponse.getHits().getAt(0).getSortValues()[0], closeTo(GeoDistance.ARC.calculate(2, 2, 1, 2, DistanceUnit.METERS), 1.e-1));
        assertThat((Double) searchResponse.getHits().getAt(1).getSortValues()[0], closeTo(GeoDistance.ARC.calculate(2, 2, 1, 1, DistanceUnit.METERS), 1.e-1));
    }

    protected void createQPoints(List<String> qHashes, List<GeoPoint> qPoints) {
        GeoPoint[] qp = {new GeoPoint(2, 1), new GeoPoint(2, 2), new GeoPoint(2, 3), new GeoPoint(2, 4)};
        qPoints.addAll(Arrays.asList(qp));
        String[] qh = {"s02equ04ven0", "s037ms06g7h0", "s065kk0dc540", "s06g7h0dyg00"};
        qHashes.addAll(Arrays.asList(qh));
    }

    public void testCrossIndexIgnoreUnmapped() throws Exception {
        assertAcked(prepareCreate("test1").addMapping(
                "type", "str_field", "type=keyword",
                "long_field", "type=long",
                "double_field", "type=double").get());
        assertAcked(prepareCreate("test2").get());

        indexRandom(true,
                client().prepareIndex("test1", "type").setSource("str_field", "bcd", "long_field", 3, "double_field", 0.65),
                client().prepareIndex("test2", "type").setSource());

        SearchResponse resp = client().prepareSearch("test1", "test2")
                .addSort(fieldSort("str_field").order(SortOrder.ASC).unmappedType("keyword"))
                .addSort(fieldSort("str_field2").order(SortOrder.DESC).unmappedType("keyword")).get();

        assertSortValues(resp,
                new Object[] {"bcd", null},
                new Object[] {null, null});

        resp = client().prepareSearch("test1", "test2")
                .addSort(fieldSort("long_field").order(SortOrder.ASC).unmappedType("long"))
                .addSort(fieldSort("long_field2").order(SortOrder.DESC).unmappedType("long")).get();
        assertSortValues(resp,
                new Object[] {3L, Long.MIN_VALUE},
                new Object[] {Long.MAX_VALUE, Long.MIN_VALUE});

        resp = client().prepareSearch("test1", "test2")
                .addSort(fieldSort("double_field").order(SortOrder.ASC).unmappedType("double"))
                .addSort(fieldSort("double_field2").order(SortOrder.DESC).unmappedType("double")).get();
        assertSortValues(resp,
                new Object[] {0.65, Double.NEGATIVE_INFINITY},
                new Object[] {Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY});
    }
}
