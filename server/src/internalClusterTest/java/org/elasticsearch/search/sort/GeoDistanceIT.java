/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertOrderedSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class GeoDistanceIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testDistanceSortingMVFields() throws Exception {
        IndexVersion version = IndexVersionUtils.randomCompatibleWriteVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("locations")
            .field("type", "geo_point");
        xContentBuilder.field("ignore_malformed", true).endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(xContentBuilder));
        ensureGreen();

        prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("names", "New York")
                    .startObject("locations")
                    .field("lat", 40.7143528)
                    .field("lon", -74.0059731)
                    .endObject()
                    .endObject()
            )
            .get();

        prepareIndex("test").setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("names", "New York 2")
                    .startObject("locations")
                    .field("lat", 400.7143528)
                    .field("lon", 285.9990269)
                    .endObject()
                    .endObject()
            )
            .get();

        prepareIndex("test").setId("3")
            .setSource(
                jsonBuilder().startObject()
                    .array("names", "Times Square", "Tribeca")
                    .startArray("locations")
                    // to NY: 5.286 km
                    .startObject()
                    .field("lat", 40.759011)
                    .field("lon", -73.9844722)
                    .endObject()
                    // to NY: 0.4621 km
                    .startObject()
                    .field("lat", 40.718266)
                    .field("lon", -74.007819)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        prepareIndex("test").setId("4")
            .setSource(
                jsonBuilder().startObject()
                    .array("names", "Wall Street", "Soho")
                    .startArray("locations")
                    // to NY: 1.055 km
                    .startObject()
                    .field("lat", 40.7051157)
                    .field("lon", -74.0088305)
                    .endObject()
                    // to NY: 1.258 km
                    .startObject()
                    .field("lat", 40.7247222)
                    .field("lon", -74)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        prepareIndex("test").setId("5")
            .setSource(
                jsonBuilder().startObject()
                    .array("names", "Greenwich Village", "Brooklyn")
                    .startArray("locations")
                    // to NY: 2.029 km
                    .startObject()
                    .field("lat", 40.731033)
                    .field("lon", -73.9962255)
                    .endObject()
                    // to NY: 8.572 km
                    .startObject()
                    .field("lat", 40.65)
                    .field("lon", -73.95)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        indicesAdmin().prepareRefresh().get();

        // Order: Asc
        assertResponse(
            prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.ASC)),
            response -> {
                assertHitCount(response, 5);
                assertOrderedSearchHits(response, "1", "2", "3", "4", "5");
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
                assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
                assertThat(((Number) response.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(1055.0d, 10d));
                assertThat(((Number) response.getHits().getAt(4).getSortValues()[0]).doubleValue(), closeTo(2029.0d, 10d));
            }
        );
        // Order: Asc, Mode: max
        assertResponse(
            prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.ASC).sortMode(SortMode.MAX)),
            response -> {
                assertHitCount(response, 5);
                assertOrderedSearchHits(response, "1", "2", "4", "3", "5");
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
                assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(1258.0d, 10d));
                assertThat(((Number) response.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(5286.0d, 10d));
                assertThat(((Number) response.getHits().getAt(4).getSortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));
            }
        );
        // Order: Desc
        assertResponse(
            prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.DESC)),
            response -> {
                assertHitCount(response, 5);
                assertOrderedSearchHits(response, "5", "3", "4", "2", "1");
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(5286.0d, 10d));
                assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(1258.0d, 10d));
                assertThat(((Number) response.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
                assertThat(((Number) response.getHits().getAt(4).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));
            }
        );
        // Order: Desc, Mode: min
        assertResponse(
            prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.DESC).sortMode(SortMode.MIN)),
            response -> {
                assertHitCount(response, 5);
                assertOrderedSearchHits(response, "5", "4", "3", "2", "1");
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(2029.0d, 10d));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(1055.0d, 10d));
                assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
                assertThat(((Number) response.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
                assertThat(((Number) response.getHits().getAt(4).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));
            }
        );
        assertResponse(
            prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).sortMode(SortMode.AVG).order(SortOrder.ASC)),
            response -> {
                assertHitCount(response, 5);
                assertOrderedSearchHits(response, "1", "2", "4", "3", "5");
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
                assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(1157d, 10d));
                assertThat(((Number) response.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(2874d, 10d));
                assertThat(((Number) response.getHits().getAt(4).getSortValues()[0]).doubleValue(), closeTo(5301d, 10d));
            }
        );
        assertResponse(
            prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).sortMode(SortMode.AVG).order(SortOrder.DESC)),
            response -> {
                assertHitCount(response, 5);
                assertOrderedSearchHits(response, "5", "3", "4", "2", "1");
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(5301.0d, 10d));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(2874.0d, 10d));
                assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(1157.0d, 10d));
                assertThat(((Number) response.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
                assertThat(((Number) response.getHits().getAt(4).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));
            }
        );
        try {
            prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).sortMode(SortMode.SUM));
            fail("sum should not be supported for sorting by geo distance");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    // Regression bug:
    // https://github.com/elastic/elasticsearch/issues/2851
    public void testDistanceSortingWithMissingGeoPoint() throws Exception {
        IndexVersion version = IndexVersionUtils.randomCompatibleWriteVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("locations")
            .field("type", "geo_point");
        xContentBuilder.endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(xContentBuilder));
        ensureGreen();

        prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .array("names", "Times Square", "Tribeca")
                    .startArray("locations")
                    // to NY: 5.286 km
                    .startObject()
                    .field("lat", 40.759011)
                    .field("lon", -73.9844722)
                    .endObject()
                    // to NY: 0.4621 km
                    .startObject()
                    .field("lat", 40.718266)
                    .field("lon", -74.007819)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        prepareIndex("test").setId("2").setSource(jsonBuilder().startObject().array("names", "Wall Street", "Soho").endObject()).get();

        refresh();

        // Order: Asc
        assertResponse(
            prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.ASC)),
            response -> {
                assertHitCount(response, 2);
                assertOrderedSearchHits(response, "1", "2");
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
            }
        );
        // Order: Desc
        assertResponse(
            prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.DESC)),
            response -> {
                // Doc with missing geo point is first, is consistent with 0.20.x
                assertHitCount(response, 2);
                assertOrderedSearchHits(response, "2", "1");
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(5286d, 10d));
            }
        );
    }

    public void testDistanceSortingNestedFields() throws Exception {
        IndexVersion version = IndexVersionUtils.randomCompatibleWriteVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("name")
            .field("type", "text")
            .endObject()
            .startObject("branches")
            .field("type", "nested")
            .startObject("properties")
            .startObject("name")
            .field("type", "text")
            .endObject()
            .startObject("location")
            .field("type", "geo_point");
        xContentBuilder.endObject().endObject().endObject().endObject().endObject().endObject();

        assertAcked(prepareCreate("companies").setSettings(settings).setMapping(xContentBuilder));
        ensureGreen();

        indexRandom(
            true,
            prepareIndex("companies").setId("1")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "company 1")
                        .startArray("branches")
                        .startObject()
                        .field("name", "New York")
                        .startObject("location")
                        .field("lat", 40.7143528)
                        .field("lon", -74.0059731)
                        .endObject()
                        .endObject()
                        .endArray()
                        .endObject()
                ),
            prepareIndex("companies").setId("2")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "company 2")
                        .startArray("branches")
                        .startObject()
                        .field("name", "Times Square")
                        .startObject("location")
                        .field("lat", 40.759011)
                        .field("lon", -73.9844722)
                        .endObject() // to NY: 5.286 km
                        .endObject()
                        .startObject()
                        .field("name", "Tribeca")
                        .startObject("location")
                        .field("lat", 40.718266)
                        .field("lon", -74.007819)
                        .endObject() // to NY:
                                     // 0.4621
                                     // km
                        .endObject()
                        .endArray()
                        .endObject()
                ),
            prepareIndex("companies").setId("3")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "company 3")
                        .startArray("branches")
                        .startObject()
                        .field("name", "Wall Street")
                        .startObject("location")
                        .field("lat", 40.7051157)
                        .field("lon", -74.0088305)
                        .endObject() // to NY: 1.055 km
                        .endObject()
                        .startObject()
                        .field("name", "Soho")
                        .startObject("location")
                        .field("lat", 40.7247222)
                        .field("lon", -74)
                        .endObject() // to NY: 1.258
                                     // km
                        .endObject()
                        .endArray()
                        .endObject()
                ),
            prepareIndex("companies").setId("4")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "company 4")
                        .startArray("branches")
                        .startObject()
                        .field("name", "Greenwich Village")
                        .startObject("location")
                        .field("lat", 40.731033)
                        .field("lon", -73.9962255)
                        .endObject() // to NY:
                                     // 2.029
                                     // km
                        .endObject()
                        .startObject()
                        .field("name", "Brooklyn")
                        .startObject("location")
                        .field("lat", 40.65)
                        .field("lon", -73.95)
                        .endObject() // to NY:
                                     // 8.572 km
                        .endObject()
                        .endArray()
                        .endObject()
                )
        );

        // Order: Asc
        assertResponse(
            prepareSearch("companies").setQuery(matchAllQuery())
                .addSort(
                    SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731)
                        .order(SortOrder.ASC)
                        .setNestedSort(new NestedSortBuilder("branches"))
                ),
            response -> {
                assertHitCount(response, 4);
                assertOrderedSearchHits(response, "1", "2", "3", "4");
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
                assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(1055.0d, 10d));
                assertThat(((Number) response.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(2029.0d, 10d));
            }
        );
        // Order: Asc, Mode: max
        assertResponse(
            prepareSearch("companies").setQuery(matchAllQuery())
                .addSort(
                    SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731)
                        .order(SortOrder.ASC)
                        .sortMode(SortMode.MAX)
                        .setNestedSort(new NestedSortBuilder("branches"))
                ),
            response -> {
                assertHitCount(response, 4);
                assertOrderedSearchHits(response, "1", "3", "2", "4");
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(1258.0d, 10d));
                assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(5286.0d, 10d));
                assertThat(((Number) response.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));
            }
        );
        // Order: Desc
        assertResponse(
            prepareSearch("companies").setQuery(matchAllQuery())
                .addSort(
                    SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731)
                        .order(SortOrder.DESC)
                        .setNestedSort(new NestedSortBuilder("branches"))
                ),
            response -> {
                assertHitCount(response, 4);
                assertOrderedSearchHits(response, "4", "2", "3", "1");
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(5286.0d, 10d));
                assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(1258.0d, 10d));
                assertThat(((Number) response.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));
            }
        );
        // Order: Desc, Mode: min
        assertResponse(
            prepareSearch("companies").setQuery(matchAllQuery())
                .addSort(
                    SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731)
                        .order(SortOrder.DESC)
                        .sortMode(SortMode.MIN)
                        .setNestedSort(new NestedSortBuilder("branches"))
                ),
            response -> {
                assertHitCount(response, 4);
                assertOrderedSearchHits(response, "4", "3", "2", "1");
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(2029.0d, 10d));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(1055.0d, 10d));
                assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
                assertThat(((Number) response.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));
            }
        );
        assertResponse(
            prepareSearch("companies").setQuery(matchAllQuery())
                .addSort(
                    SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731)
                        .sortMode(SortMode.AVG)
                        .order(SortOrder.ASC)
                        .setNestedSort(new NestedSortBuilder("branches"))
                ),
            response -> {
                assertHitCount(response, 4);
                assertOrderedSearchHits(response, "1", "3", "2", "4");
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(1157.0d, 10d));
                assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(2874.0d, 10d));
                assertThat(((Number) response.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(5301.0d, 10d));
            }
        );
        assertResponse(
            prepareSearch("companies").setQuery(matchAllQuery())
                .addSort(
                    SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731)
                        .setNestedSort(new NestedSortBuilder("branches"))
                        .sortMode(SortMode.AVG)
                        .order(SortOrder.DESC)
                ),
            response -> {
                assertHitCount(response, 4);
                assertOrderedSearchHits(response, "4", "2", "3", "1");
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(5301.0d, 10d));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(2874.0d, 10d));
                assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(1157.0d, 10d));
                assertThat(((Number) response.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));
            }
        );
        assertResponse(
            prepareSearch("companies").setQuery(matchAllQuery())
                .addSort(
                    SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731)
                        .setNestedSort(new NestedSortBuilder("branches").setFilter(termQuery("branches.name", "brooklyn")))
                        .sortMode(SortMode.AVG)
                        .order(SortOrder.ASC)
                ),
            response -> {
                assertHitCount(response, 4);
                assertFirstHit(response, hasId("4"));
                assertSearchHits(response, "1", "2", "3", "4");
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
                assertThat(((Number) response.getHits().getAt(2).getSortValues()[0]).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
                assertThat(((Number) response.getHits().getAt(3).getSortValues()[0]).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
            }
        );
        try {
            prepareSearch("companies").setQuery(matchAllQuery())
                .addSort(
                    SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731)
                        .sortMode(SortMode.SUM)
                        .setNestedSort(new NestedSortBuilder("branches"))
                );
            fail("Sum should not be allowed as sort mode");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    /**
     * Issue 3073
     */
    public void testGeoDistanceFilter() throws IOException {
        IndexVersion version = IndexVersionUtils.randomCompatibleWriteVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        double lat = 40.720611;
        double lon = -73.998776;

        XContentBuilder mapping = JsonXContent.contentBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("pin")
            .field("type", "geo_point");
        mapping.endObject().endObject().endObject().endObject();

        XContentBuilder source = JsonXContent.contentBuilder().startObject().field("pin", Geohash.stringEncode(lon, lat)).endObject();

        assertAcked(prepareCreate("locations").setSettings(settings).setMapping(mapping));
        prepareIndex("locations").setId("1").setCreate(true).setSource(source).get();
        refresh();
        client().prepareGet("locations", "1").get();

        assertHitCount(
            prepareSearch("locations").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(QueryBuilders.geoDistanceQuery("pin").geoDistance(GeoDistance.ARC).point(lat, lon).distance("1m")),
            1
        );
    }

    public void testDistanceSortingWithUnmappedField() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("locations")
            .field("type", "geo_point");
        xContentBuilder.endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test1").setMapping(xContentBuilder));
        assertAcked(prepareCreate("test2"));
        ensureGreen();

        prepareIndex("test1").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .array("names", "Times Square", "Tribeca")
                    .startArray("locations")
                    // to NY: 5.286 km
                    .startObject()
                    .field("lat", 40.759011)
                    .field("lon", -73.9844722)
                    .endObject()
                    // to NY: 0.4621 km
                    .startObject()
                    .field("lat", 40.718266)
                    .field("lon", -74.007819)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        prepareIndex("test2").setId("2").setSource(jsonBuilder().startObject().array("names", "Wall Street", "Soho").endObject()).get();

        refresh();

        // Order: Asc
        assertResponse(
            prepareSearch("test1", "test2").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).ignoreUnmapped(true).order(SortOrder.ASC)),
            response -> {
                assertHitCount(response, 2);
                assertOrderedSearchHits(response, "1", "2");
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
            }
        );
        // Order: Desc
        assertResponse(
            prepareSearch("test1", "test2").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).ignoreUnmapped(true).order(SortOrder.DESC)),
            response -> {
                // Doc with missing geo point is first, is consistent with 0.20.x
                assertHitCount(response, 2);
                assertOrderedSearchHits(response, "2", "1");
                assertThat(((Number) response.getHits().getAt(0).getSortValues()[0]).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
                assertThat(((Number) response.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(5286d, 10d));
            }
        );
        // Make sure that by default the unmapped fields continue to fail
        assertResponse(
            prepareSearch("test1", "test2").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.DESC)),
            response -> {
                assertThat(response.getFailedShards(), greaterThan(0));
                assertHitCount(response, 1);
            }
        );
    }
}
