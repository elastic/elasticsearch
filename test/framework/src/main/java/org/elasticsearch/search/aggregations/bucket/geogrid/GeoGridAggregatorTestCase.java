/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public abstract class GeoGridAggregatorTestCase<T extends InternalGeoGridBucket> extends AggregatorTestCase {

    private static final String FIELD_NAME = "location";

    /**
     * Generate a random precision according to the rules of the given aggregation.
     */
    protected abstract int randomPrecision();

    /**
     * Convert geo point into a hash string (bucket string ID)
     */
    protected abstract String hashAsString(double lng, double lat, int precision);

    /**
     * Create a new named {@link GeoGridAggregationBuilder}-derived builder
     */
    protected abstract GeoGridAggregationBuilder createBuilder(String name);

    /**
     * Return a point within the bounds of the tile grid
     */
    protected abstract Point randomPoint();

    /**
     * Return a random {@link GeoBoundingBox} within the bounds of the tile grid.
     */
    protected abstract GeoBoundingBox randomBBox();

    /**
     * Return the bounding tile as a {@link Rectangle} for a given point
     */
    protected abstract Rectangle getTile(double lng, double lat, int precision);

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return createBuilder("foo").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CoreValuesSourceType.GEOPOINT);
    }

    public void testNoDocs() throws IOException {
        testCase(
            new MatchAllDocsQuery(),
            FIELD_NAME,
            randomPrecision(),
            null,
            geoGrid -> { assertEquals(0, geoGrid.getBuckets().size()); },
            iw -> {
                // Intentionally not writing any docs
            }
        );
    }

    public void testUnmapped() throws IOException {
        testCase(
            new MatchAllDocsQuery(),
            "wrong_field",
            randomPrecision(),
            null,
            geoGrid -> { assertEquals(0, geoGrid.getBuckets().size()); },
            iw -> {
                iw.addDocument(Collections.singleton(new LatLonDocValuesField(FIELD_NAME, 10D, 10D)));
            }
        );
    }

    public void testUnmappedMissing() throws IOException {
        GeoGridAggregationBuilder builder = createBuilder("_name").field("wrong_field").missing("53.69437,6.475031");
        testCase(
            new MatchAllDocsQuery(),
            randomPrecision(),
            null,
            geoGrid -> assertEquals(1, geoGrid.getBuckets().size()),
            iw -> iw.addDocument(Collections.singleton(new LatLonDocValuesField(FIELD_NAME, 10D, 10D))),
            builder
        );

    }

    public void testSingletonDocs() throws IOException {
        testWithSeveralDocs(() -> true, null);
    }

    public void testBoundedSingletonDocs() throws IOException {
        testWithSeveralDocs(() -> true, randomBBox());
    }

    public void testMultiValuedDocs() throws IOException {
        testWithSeveralDocs(LuceneTestCase::rarely, null);
    }

    public void testBoundedMultiValuedDocs() throws IOException {
        testWithSeveralDocs(LuceneTestCase::rarely, randomBBox());
    }

    private void testWithSeveralDocs(BooleanSupplier supplier, GeoBoundingBox bbox) throws IOException {
        int precision = randomPrecision();
        int numPoints = randomIntBetween(8, 128);
        Map<String, Integer> expectedCountPerGeoHash = new HashMap<>();
        testCase(new MatchAllDocsQuery(), FIELD_NAME, precision, bbox, geoHashGrid -> {
            assertEquals(expectedCountPerGeoHash.size(), geoHashGrid.getBuckets().size());
            for (GeoGrid.Bucket bucket : geoHashGrid.getBuckets()) {
                assertEquals((long) expectedCountPerGeoHash.get(bucket.getKeyAsString()), bucket.getDocCount());
            }
            if (bbox == null) {
                assertTrue(AggregationInspectionHelper.hasValue(geoHashGrid));
            }
        }, iw -> {
            List<LatLonDocValuesField> points = new ArrayList<>();
            Set<String> distinctHashesPerDoc = new HashSet<>();
            for (int pointId = 0; pointId < numPoints; pointId++) {
                double[] latLng = randomLatLng();
                points.add(new LatLonDocValuesField(FIELD_NAME, latLng[0], latLng[1]));
                String hash = hashAsString(latLng[1], latLng[0], precision);
                Rectangle bin = getTile(latLng[1], latLng[0], precision);
                if (intersectsBounds(bin, bbox) || validPoint(latLng[1], latLng[0], bbox)) {
                    if (distinctHashesPerDoc.contains(hash) == false) {
                        expectedCountPerGeoHash.put(hash, expectedCountPerGeoHash.getOrDefault(hash, 0) + 1);
                    }
                    distinctHashesPerDoc.add(hash);
                }
                if (supplier.getAsBoolean()) {
                    iw.addDocument(points);
                    points.clear();
                    distinctHashesPerDoc.clear();
                }
            }
            if (points.size() != 0) {
                iw.addDocument(points);
            }
        });
    }

    public void testSingletonDocsAsSubAgg() throws IOException {
        testWithSeveralDocsAsSubAgg(() -> true, null);
    }

    public void testBoundedSingletonDocsAsSubAgg() throws IOException {
        testWithSeveralDocsAsSubAgg(() -> true, randomBBox());
    }

    public void testMultiValuedDocsAsSubAgg() throws IOException {
        testWithSeveralDocsAsSubAgg(LuceneTestCase::rarely, null);
    }

    public void testBoundedMultiValuedDocsAsSubAgg() throws IOException {
        testWithSeveralDocsAsSubAgg(LuceneTestCase::rarely, randomBBox());
    }

    private void testWithSeveralDocsAsSubAgg(BooleanSupplier supplier, GeoBoundingBox bbox) throws IOException {
        int precision = randomPrecision();
        int numPoints = randomIntBetween(8, 128);
        Map<String, Map<String, Long>> expectedCountPerTPerGeoHash = new TreeMap<>();
        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("t").field("t").size(numPoints);
        GeoGridAggregationBuilder gridBuilder = createBuilder("gg").field(FIELD_NAME).precision(precision);
        if (bbox != null) {
            gridBuilder.setGeoBoundingBox(bbox);
        }
        aggregationBuilder.subAggregation(gridBuilder);
        testCase(iw -> {
            List<IndexableField> fields = new ArrayList<>();
            Set<String> distinctHashesPerDoc = new HashSet<>();
            String t = randomAlphaOfLength(1);
            for (int pointId = 0; pointId < numPoints; pointId++) {
                Map<String, Long> expectedCountPerGeoHash = expectedCountPerTPerGeoHash.computeIfAbsent(t, k -> new TreeMap<>());
                double[] latLng = randomLatLng();
                fields.add(new LatLonDocValuesField(FIELD_NAME, latLng[0], latLng[1]));
                String hash = hashAsString(latLng[1], latLng[0], precision);
                if (distinctHashesPerDoc.contains(hash) == false) {
                    if (intersectsBounds(getTile(latLng[1], latLng[0], precision), bbox) || validPoint(latLng[1], latLng[0], bbox)) {
                        expectedCountPerGeoHash.put(hash, expectedCountPerGeoHash.getOrDefault(hash, 0L) + 1);
                        distinctHashesPerDoc.add(hash);
                    }
                }
                if (supplier.getAsBoolean()) {
                    fields.add(new Field("t", new BytesRef(t), KeywordFieldMapper.Defaults.FIELD_TYPE));
                    iw.addDocument(fields);
                    fields.clear();
                    distinctHashesPerDoc.clear();
                    t = randomAlphaOfLength(1);
                }
            }
            if (fields.size() != 0) {
                fields.add(new Field("t", new BytesRef(t), KeywordFieldMapper.Defaults.FIELD_TYPE));
                iw.addDocument(fields);
            }
        }, terms -> {
            Map<String, Map<String, Long>> actual = new TreeMap<>();
            for (StringTerms.Bucket tb : ((StringTerms) terms).getBuckets()) {
                InternalGeoGrid<?> gg = tb.getAggregations().get("gg");
                Map<String, Long> sub = new TreeMap<>();
                for (InternalGeoGridBucket ggb : gg.getBuckets()) {
                    sub.put(ggb.getKeyAsString(), ggb.getDocCount());
                }
                actual.put(tb.getKeyAsString(), sub);
            }
            assertThat(actual, equalTo(expectedCountPerTPerGeoHash));
        }, new AggTestConfig(aggregationBuilder, keywordField("t"), geoPointField(FIELD_NAME)));
    }

    private double[] randomLatLng() {
        Point point = randomPoint();

        // Precision-adjust longitude/latitude to avoid wrong bucket placement
        // Internally, lat/lng get converted to 32 bit integers, loosing some precision.
        // This does not affect geohashing because geohash uses the same algorithm,
        // but it does affect other bucketing algos, thus we need to do the same steps here.
        double lon = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(point.getLon()));
        double lat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(point.getLat()));

        return new double[] { lat, lon };
    }

    private boolean validPoint(double x, double y, GeoBoundingBox bbox) {
        if (bbox == null) {
            return true;
        }
        if (bbox.top() > y && bbox.bottom() < y) {
            boolean crossesDateline = bbox.left() > bbox.right();
            if (crossesDateline) {
                return bbox.left() < x || bbox.right() > x;
            } else {
                return bbox.left() < x && bbox.right() > x;
            }
        }
        return false;
    }

    private boolean intersectsBounds(Rectangle pointTile, GeoBoundingBox bbox) {
        if (bbox == null) {
            return true;
        }
        if (pointTile.getMinX() > pointTile.getMaxX()) {
            Rectangle right = new Rectangle(pointTile.getMinX(), 180, pointTile.getMaxY(), pointTile.getMinY());
            Rectangle left = new Rectangle(-180, pointTile.getMaxX(), pointTile.getMaxY(), pointTile.getMinY());
            return intersectsBounds(left, bbox) || intersectsBounds(right, bbox);
        }
        if (bbox.top() > pointTile.getMinY() && bbox.bottom() < pointTile.getMaxY()) {
            boolean crossesDateline = bbox.left() > bbox.right();
            if (crossesDateline) {
                return bbox.left() < pointTile.getMaxX() || bbox.right() > pointTile.getMinX();
            } else {
                return bbox.left() < pointTile.getMaxX() && bbox.right() > pointTile.getMinX();
            }
        }
        return false;
    }

    protected void testCase(
        Query query,
        String field,
        int precision,
        GeoBoundingBox geoBoundingBox,
        Consumer<InternalGeoGrid<T>> verify,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex
    ) throws IOException {
        testCase(query, precision, geoBoundingBox, verify, buildIndex, createBuilder("_name").field(field));
    }

    private void testCase(
        Query query,
        int precision,
        GeoBoundingBox geoBoundingBox,
        Consumer<InternalGeoGrid<T>> verify,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        GeoGridAggregationBuilder aggregationBuilder
    ) throws IOException {
        aggregationBuilder.precision(precision);
        if (geoBoundingBox != null) {
            aggregationBuilder.setGeoBoundingBox(geoBoundingBox);
            assertThat(aggregationBuilder.geoBoundingBox(), equalTo(geoBoundingBox));
        }
        MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType(aggregationBuilder.field());
        testCase(buildIndex, verify, new AggTestConfig(aggregationBuilder, fieldType).withQuery(query));
    }

    @Override
    public void doAssertReducedMultiBucketConsumer(Aggregation agg, MultiBucketConsumerService.MultiBucketConsumer bucketConsumer) {
        /*
         * No-op.
         */
    }
}
