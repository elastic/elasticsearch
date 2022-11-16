/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.SuiteScopeTestCase
public abstract class AbstractGeoTestCase extends ESIntegTestCase {

    protected static final String SINGLE_VALUED_FIELD_NAME = "spatial_value";
    protected static final String MULTI_VALUED_FIELD_NAME = "spatial_values";
    protected static final String NUMBER_FIELD_NAME = "l_values";
    protected static final String UNMAPPED_IDX_NAME = "idx_unmapped";
    protected static final String IDX_NAME = "idx";
    protected static final String EMPTY_IDX_NAME = "empty_idx";
    protected static final String DATELINE_IDX_NAME = "dateline_idx";
    protected static final String HIGH_CARD_IDX_NAME = "high_card_idx";
    protected static final String IDX_ZERO_NAME = "idx_zero";
    protected static final double GEOHASH_TOLERANCE = 1E-5D;

    // These fields need to be static because they are shared between test instances using SuiteScopeTestCase
    protected static int numDocs;
    protected static int numUniqueGeoPoints;
    protected static SpatialPoint[] singleValues, multiValues;
    protected static SpatialPoint singleTopLeft, singleBottomRight, multiTopLeft, multiBottomRight, singleCentroid, multiCentroid,
        unmappedCentroid;
    protected static Map<String, Integer> expectedDocCountsForGeoHash = null;
    protected static Map<String, SpatialPoint> expectedCentroidsForGeoHash = null;

    // These methods allow various implementations of SpatialPoint to be tested (eg. GeoPoint and CartesianPoint)
    protected abstract String fieldTypeName();

    protected abstract SpatialPoint makePoint(double x, double y);

    protected abstract SpatialPoint randomPoint();

    protected abstract void resetX(SpatialPoint point, double x);

    protected abstract void resetY(SpatialPoint point, double y);

    protected abstract SpatialPoint reset(SpatialPoint point, double x, double y);

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex(UNMAPPED_IDX_NAME);
        assertAcked(
            prepareCreate(IDX_NAME).setMapping(
                SINGLE_VALUED_FIELD_NAME,
                "type=" + fieldTypeName(),
                MULTI_VALUED_FIELD_NAME,
                "type=" + fieldTypeName(),
                NUMBER_FIELD_NAME,
                "type=long",
                "tag",
                "type=keyword"
            )
        );

        singleTopLeft = makePoint(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        singleBottomRight = makePoint(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        multiTopLeft = makePoint(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        multiBottomRight = makePoint(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        singleCentroid = makePoint(0, 0);
        multiCentroid = makePoint(0, 0);
        unmappedCentroid = makePoint(0, 0);

        numDocs = randomIntBetween(6, 20);
        numUniqueGeoPoints = randomIntBetween(1, numDocs);
        expectedDocCountsForGeoHash = new HashMap<>(numDocs * 2);
        expectedCentroidsForGeoHash = new HashMap<>(numDocs * 2);

        singleValues = new SpatialPoint[numUniqueGeoPoints];
        for (int i = 0; i < singleValues.length; i++) {
            singleValues[i] = randomPoint();
            updateBoundsTopLeft(singleValues[i], singleTopLeft);
            updateBoundsBottomRight(singleValues[i], singleBottomRight);
        }

        multiValues = new SpatialPoint[numUniqueGeoPoints];
        for (int i = 0; i < multiValues.length; i++) {
            multiValues[i] = randomPoint();
            updateBoundsTopLeft(multiValues[i], multiTopLeft);
            updateBoundsBottomRight(multiValues[i], multiBottomRight);
        }

        List<IndexRequestBuilder> builders = new ArrayList<>();

        SpatialPoint singleVal;
        final SpatialPoint[] multiVal = new SpatialPoint[2];
        double newMVLat, newMVLon;
        for (int i = 0; i < numDocs; i++) {
            singleVal = singleValues[i % numUniqueGeoPoints];
            multiVal[0] = multiValues[i % numUniqueGeoPoints];
            multiVal[1] = multiValues[(i + 1) % numUniqueGeoPoints];
            builders.add(
                client().prepareIndex(IDX_NAME)
                    .setSource(
                        jsonBuilder().startObject()
                            .array(SINGLE_VALUED_FIELD_NAME, singleVal.getX(), singleVal.getY())
                            .startArray(MULTI_VALUED_FIELD_NAME)
                            .startArray()
                            .value(multiVal[0].getX())
                            .value(multiVal[0].getY())
                            .endArray()
                            .startArray()
                            .value(multiVal[1].getX())
                            .value(multiVal[1].getY())
                            .endArray()
                            .endArray()
                            .field(NUMBER_FIELD_NAME, i)
                            .field("tag", "tag" + i)
                            .endObject()
                    )
            );
            singleCentroid = reset(
                singleCentroid,
                singleCentroid.getX() + (singleVal.getX() - singleCentroid.getX()) / (i + 1),
                singleCentroid.getY() + (singleVal.getY() - singleCentroid.getY()) / (i + 1)
            );
            newMVLat = (multiVal[0].getY() + multiVal[1].getY()) / 2d;
            newMVLon = (multiVal[0].getX() + multiVal[1].getX()) / 2d;
            multiCentroid = reset(
                multiCentroid,
                multiCentroid.getX() + (newMVLon - multiCentroid.getX()) / (i + 1),
                multiCentroid.getY() + (newMVLat - multiCentroid.getY()) / (i + 1)
            );
        }

        assertAcked(prepareCreate(EMPTY_IDX_NAME).setMapping(SINGLE_VALUED_FIELD_NAME, "type=" + fieldTypeName()));

        assertAcked(
            prepareCreate(DATELINE_IDX_NAME).setMapping(
                SINGLE_VALUED_FIELD_NAME,
                "type=" + fieldTypeName(),
                MULTI_VALUED_FIELD_NAME,
                "type=" + fieldTypeName(),
                NUMBER_FIELD_NAME,
                "type=long",
                "tag",
                "type=keyword"
            )
        );

        SpatialPoint[] geoValues = new SpatialPoint[5];
        geoValues[0] = makePoint(178, 38);
        geoValues[1] = makePoint(-179, 12);
        geoValues[2] = makePoint(170, -24);
        geoValues[3] = makePoint(-175, 32);
        geoValues[4] = makePoint(178, -11);

        for (int i = 0; i < 5; i++) {
            builders.add(
                client().prepareIndex(DATELINE_IDX_NAME)
                    .setSource(
                        jsonBuilder().startObject()
                            .array(SINGLE_VALUED_FIELD_NAME, geoValues[i].getX(), geoValues[i].getY())
                            .field(NUMBER_FIELD_NAME, i)
                            .field("tag", "tag" + i)
                            .endObject()
                    )
            );
        }
        assertAcked(
            prepareCreate(HIGH_CARD_IDX_NAME).setSettings(Settings.builder().put("number_of_shards", 2))
                .setMapping(
                    SINGLE_VALUED_FIELD_NAME,
                    "type=" + fieldTypeName(),
                    MULTI_VALUED_FIELD_NAME,
                    "type=" + fieldTypeName(),
                    NUMBER_FIELD_NAME,
                    "type=long,store=true",
                    "tag",
                    "type=keyword"
                )
        );

        for (int i = 0; i < 2000; i++) {
            singleVal = singleValues[i % numUniqueGeoPoints];
            builders.add(
                client().prepareIndex(HIGH_CARD_IDX_NAME)
                    .setSource(
                        jsonBuilder().startObject()
                            .array(SINGLE_VALUED_FIELD_NAME, singleVal.getX(), singleVal.getY())
                            .startArray(MULTI_VALUED_FIELD_NAME)
                            .startArray()
                            .value(multiValues[i % numUniqueGeoPoints].getX())
                            .value(multiValues[i % numUniqueGeoPoints].getY())
                            .endArray()
                            .startArray()
                            .value(multiValues[(i + 1) % numUniqueGeoPoints].getX())
                            .value(multiValues[(i + 1) % numUniqueGeoPoints].getY())
                            .endArray()
                            .endArray()
                            .field(NUMBER_FIELD_NAME, i)
                            .field("tag", "tag" + i)
                            .endObject()
                    )
            );
            updateGeohashBucketsCentroid(singleVal);
        }

        builders.add(
            client().prepareIndex(IDX_ZERO_NAME)
                .setSource(jsonBuilder().startObject().array(SINGLE_VALUED_FIELD_NAME, 0.0, 1.0).endObject())
        );
        assertAcked(prepareCreate(IDX_ZERO_NAME).setMapping(SINGLE_VALUED_FIELD_NAME, "type=" + fieldTypeName()));

        indexRandom(true, builders);
        ensureSearchable();

        // Added to debug a test failure where the terms aggregation seems to be reporting two documents with the same
        // value for NUMBER_FIELD_NAME. This will check that after random indexing each document only has 1 value for
        // NUMBER_FIELD_NAME and it is the correct value. Following this initial change its seems that this call was getting
        // more that 2000 hits (actual value was 2059) so now it will also check to ensure all hits have the correct index and type.
        SearchResponse response = client().prepareSearch(HIGH_CARD_IDX_NAME)
            .addStoredField(NUMBER_FIELD_NAME)
            .addSort(SortBuilders.fieldSort(NUMBER_FIELD_NAME).order(SortOrder.ASC))
            .setSize(5000)
            .get();
        assertSearchResponse(response);
        long totalHits = response.getHits().getTotalHits().value;
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        logger.info("Full high_card_idx Response Content:\n{ {} }", Strings.toString(builder));
        for (int i = 0; i < totalHits; i++) {
            SearchHit searchHit = response.getHits().getAt(i);
            assertThat("Hit " + i + " with id: " + searchHit.getId(), searchHit.getIndex(), equalTo("high_card_idx"));
            DocumentField hitField = searchHit.field(NUMBER_FIELD_NAME);

            assertThat("Hit " + i + " has wrong number of values", hitField.getValues().size(), equalTo(1));
            Long value = hitField.getValue();
            assertThat("Hit " + i + " has wrong value", value.intValue(), equalTo(i));
        }
        assertThat(totalHits, equalTo(2000L));
    }

    private void updateGeohashBucketsCentroid(final SpatialPoint location) {
        String hash = Geohash.stringEncode(location.getX(), location.getY(), Geohash.PRECISION);
        for (int precision = Geohash.PRECISION; precision > 0; --precision) {
            final String h = hash.substring(0, precision);
            expectedDocCountsForGeoHash.put(h, expectedDocCountsForGeoHash.getOrDefault(h, 0) + 1);
            expectedCentroidsForGeoHash.put(h, updateHashCentroid(h, location));
        }
    }

    private SpatialPoint updateHashCentroid(String hash, final SpatialPoint location) {
        SpatialPoint centroid = expectedCentroidsForGeoHash.getOrDefault(hash, null);
        if (centroid == null) {
            return makePoint(location.getX(), location.getY());
        }
        final int docCount = expectedDocCountsForGeoHash.get(hash);
        final double newLon = centroid.getX() + (location.getX() - centroid.getX()) / docCount;
        final double newLat = centroid.getY() + (location.getY() - centroid.getY()) / docCount;
        return reset(centroid, newLon, newLat);
    }

    private void updateBoundsBottomRight(SpatialPoint point, SpatialPoint currentBound) {
        if (point.getY() < currentBound.getY()) {
            resetY(currentBound, point.getY());
        }
        if (point.getX() > currentBound.getX()) {
            resetX(currentBound, point.getX());
        }
    }

    private void updateBoundsTopLeft(SpatialPoint point, SpatialPoint currentBound) {
        if (point.getY() > currentBound.getY()) {
            resetY(currentBound, point.getY());
        }
        if (point.getX() < currentBound.getX()) {
            resetX(currentBound, point.getX());
        }
    }
}
