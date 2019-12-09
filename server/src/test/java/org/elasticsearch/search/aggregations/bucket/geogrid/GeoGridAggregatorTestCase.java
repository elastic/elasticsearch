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
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), FIELD_NAME, randomPrecision(), null, geoGrid -> {
            assertEquals(0, geoGrid.getBuckets().size());
        }, iw -> {
            // Intentionally not writing any docs
        });
    }

    public void testFieldMissing() throws IOException {
        testCase(new MatchAllDocsQuery(), "wrong_field", randomPrecision(), null, geoGrid -> {
            assertEquals(0, geoGrid.getBuckets().size());
        }, iw -> {
            iw.addDocument(Collections.singleton(new LatLonDocValuesField(FIELD_NAME, 10D, 10D)));
        });
    }

    public void testWithSeveralDocs() throws IOException {
        int precision = randomPrecision();
        int numPoints = randomIntBetween(8, 128);
        Map<String, Integer> expectedCountPerGeoHash = new HashMap<>();
        testCase(new MatchAllDocsQuery(), FIELD_NAME, precision, null, geoHashGrid -> {
            assertEquals(expectedCountPerGeoHash.size(), geoHashGrid.getBuckets().size());
            for (GeoGrid.Bucket bucket : geoHashGrid.getBuckets()) {
                assertEquals((long) expectedCountPerGeoHash.get(bucket.getKeyAsString()), bucket.getDocCount());
            }
            assertTrue(AggregationInspectionHelper.hasValue(geoHashGrid));
        }, iw -> {
            List<LatLonDocValuesField> points = new ArrayList<>();
            Set<String> distinctHashesPerDoc = new HashSet<>();
            for (int pointId = 0; pointId < numPoints; pointId++) {
                double lat = (180d * randomDouble()) - 90d;
                double lng = (360d * randomDouble()) - 180d;

                // Precision-adjust longitude/latitude to avoid wrong bucket placement
                // Internally, lat/lng get converted to 32 bit integers, loosing some precision.
                // This does not affect geohashing because geohash uses the same algorithm,
                // but it does affect other bucketing algos, thus we need to do the same steps here.
                lng = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lng));
                lat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));

                points.add(new LatLonDocValuesField(FIELD_NAME, lat, lng));
                String hash = hashAsString(lng, lat, precision);
                if (distinctHashesPerDoc.contains(hash) == false) {
                    expectedCountPerGeoHash.put(hash, expectedCountPerGeoHash.getOrDefault(hash, 0) + 1);
                }
                distinctHashesPerDoc.add(hash);
                if (usually()) {
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

    public void testBounds() throws IOException {
        final int numDocs = randomIntBetween(64, 256);
        final GeoGridAggregationBuilder builder = createBuilder("_name");

        expectThrows(IllegalArgumentException.class, () -> builder.precision(-1));
        expectThrows(IllegalArgumentException.class, () -> builder.precision(30));

        Rectangle rectangle = GeometryTestUtils.randomRectangle();

        int in = 0, out = 0;
        List<LatLonDocValuesField> docs = new ArrayList<>();
        while (in + out < numDocs) {
            if (rectangle.getMinX() > rectangle.getMaxX()) {
                if (randomBoolean()) {
                    double lonWithin = randomBoolean() ?
                        randomDoubleBetween(rectangle.getMinX(), 180.0, true)
                        : randomDoubleBetween(-180.0, rectangle.getMaxX(), true);
                    double latWithin = randomDoubleBetween(rectangle.getMinY(), rectangle.getMaxY(), true);
                    in++;
                    docs.add(new LatLonDocValuesField(FIELD_NAME, latWithin, lonWithin));
                } else {
                    double lonOutside = (rectangle.getMinX() + rectangle.getMaxX()) / 2;
                    double latOutside = rectangle.getMinX() - randomIntBetween(1, 10);
                    out++;
                    docs.add(new LatLonDocValuesField(FIELD_NAME, latOutside, lonOutside));
                }

            } else {
                if (randomBoolean()) {
                    double lonWithin = randomDoubleBetween(rectangle.getMinX(), rectangle.getMaxX(), true);
                    double latWithin = randomDoubleBetween(rectangle.getMinY(), rectangle.getMaxY(), true);
                    in++;
                    docs.add(new LatLonDocValuesField(FIELD_NAME, latWithin, lonWithin));
                } else {
                    double lonOutside = randomDoubleBetween(rectangle.getMaxX(), 180.0, false);
                    double latOutside = randomDoubleBetween(rectangle.getMaxY(), 90.0, false);
                    out++;
                    docs.add(new LatLonDocValuesField(FIELD_NAME, latOutside, lonOutside));
                }
            }

        }

        final long numDocsInBucket = in;

        testCase(new MatchAllDocsQuery(), FIELD_NAME, 0, rectangle, geoGrid -> {
            assertThat(geoGrid.getBuckets().size(), equalTo(1));
            long docCount = geoGrid.getBuckets().get(0).getDocCount();
            assertThat(docCount, equalTo(numDocsInBucket));
            assertTrue(AggregationInspectionHelper.hasValue(geoGrid));
        }, iw -> {
            for (LatLonDocValuesField docField : docs) {
                iw.addDocument(Collections.singletonList(docField));
            }
        });
    }

    private void testCase(Query query, String field, int precision, Rectangle boundingBox,
                          Consumer<InternalGeoGrid<T>> verify,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex) throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        GeoGridAggregationBuilder aggregationBuilder = createBuilder("_name").field(field);
        aggregationBuilder.precision(precision);
        if (boundingBox != null) {
            GeoBoundingBox geoBoundingBox = new GeoBoundingBox(new GeoPoint(boundingBox.getMaxLat(), boundingBox.getMinLon()),
                new GeoPoint(boundingBox.getMinLat(), boundingBox.getMaxLon()));
            aggregationBuilder.setGeoBoundingBox(geoBoundingBox);
            assertThat(aggregationBuilder.geoBoundingBox(), equalTo(geoBoundingBox));
        }

        MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType();
        fieldType.setHasDocValues(true);
        fieldType.setName(FIELD_NAME);

        Aggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(query, aggregator);
        aggregator.postCollection();
        verify.accept((InternalGeoGrid<T>) aggregator.buildAggregation(0L));

        indexReader.close();
        directory.close();
    }
}
