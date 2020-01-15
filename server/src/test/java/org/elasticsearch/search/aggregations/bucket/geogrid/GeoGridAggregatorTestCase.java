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

import org.apache.lucene.document.Document;
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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.geo.CentroidCalculator;
import org.elasticsearch.common.geo.GeoShapeCoordinateEncoder;
import org.elasticsearch.common.geo.GeoTestUtils;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.fielddata.MultiGeoValues;
import org.elasticsearch.index.mapper.BinaryGeoShapeDocValuesField;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.common.geo.GeoTestUtils.triangleTreeReader;

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
     *  return which geogrid tiler is used
     */
    protected abstract GeoGridTiler geoGridTiler();

    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), FIELD_NAME, randomPrecision(), iw -> {
            // Intentionally not writing any docs
        }, geoGrid -> {
            assertEquals(0, geoGrid.getBuckets().size());
        }, new GeoPointFieldMapper.GeoPointFieldType(), new NoneCircuitBreakerService());

        testCase(new MatchAllDocsQuery(), FIELD_NAME, randomPrecision(), iw -> {
            // Intentionally not writing any docs
        }, geoGrid -> {
            assertEquals(0, geoGrid.getBuckets().size());
        }, new GeoShapeFieldMapper.GeoShapeFieldType(), new NoneCircuitBreakerService());
    }

    public void testFieldMissing() throws IOException {
        testCase(new MatchAllDocsQuery(), "wrong_field", randomPrecision(), iw -> {
            iw.addDocument(Collections.singleton(new LatLonDocValuesField(FIELD_NAME, 10D, 10D)));
        }, geoGrid -> {
            assertEquals(0, geoGrid.getBuckets().size());
        }, new GeoPointFieldMapper.GeoPointFieldType(), new NoneCircuitBreakerService());

        testCase(new MatchAllDocsQuery(), "wrong_field", randomPrecision(), iw -> {
            iw.addDocument(Collections.singleton(
                new BinaryGeoShapeDocValuesField(FIELD_NAME, GeoTestUtils.toDecodedTriangles(new Point(10D, 10D)),
                    new CentroidCalculator(new Point(10D, 10D)))));
        }, geoGrid -> {
            assertEquals(0, geoGrid.getBuckets().size());
        }, new GeoShapeFieldMapper.GeoShapeFieldType(), new NoneCircuitBreakerService());
    }

    public void testGeoPointWithSeveralDocs() throws IOException {
        int precision = randomPrecision();
        int numPoints = randomIntBetween(8, 128);
        Map<String, Integer> expectedCountPerGeoHash = new HashMap<>();
        testCase(new MatchAllDocsQuery(), FIELD_NAME, precision, iw -> {
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
        }, geoHashGrid -> {
            assertEquals(expectedCountPerGeoHash.size(), geoHashGrid.getBuckets().size());
            for (GeoGrid.Bucket bucket : geoHashGrid.getBuckets()) {
                assertEquals((long) expectedCountPerGeoHash.get(bucket.getKeyAsString()), bucket.getDocCount());
            }
            assertTrue(AggregationInspectionHelper.hasValue(geoHashGrid));
        }, new GeoPointFieldMapper.GeoPointFieldType(), new NoneCircuitBreakerService());
    }

    public void testGeoShapeWithSeveralDocs() throws IOException {
        int precision = randomIntBetween(1, 4);
        int numShapes = randomIntBetween(8, 128);
        Map<String, Integer> expectedCountPerGeoHash = new HashMap<>();
        testCase(new MatchAllDocsQuery(), FIELD_NAME, precision, iw -> {
            List<Point> shapes = new ArrayList<>();
            Document document = new Document();
            Set<String> distinctHashesPerDoc = new HashSet<>();
            for (int shapeId = 0; shapeId < numShapes; shapeId++) {
                // undefined close to pole
                double lat = (170.10225756d * randomDouble()) - 85.05112878d;
                double lng = (360d * randomDouble()) - 180d;

                // Precision-adjust longitude/latitude to avoid wrong bucket placement
                // Internally, lat/lng get converted to 32 bit integers, loosing some precision.
                // This does not affect geohashing because geohash uses the same algorithm,
                // but it does affect other bucketing algos, thus we need to do the same steps here.
                lng = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lng));
                lat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));

                shapes.add(new Point(lng, lat));
                String hash = hashAsString(lng, lat, precision);
                if (distinctHashesPerDoc.contains(hash) == false) {
                    expectedCountPerGeoHash.put(hash, expectedCountPerGeoHash.getOrDefault(hash, 0) + 1);
                }
                distinctHashesPerDoc.add(hash);
                if (usually()) {
                    Geometry geometry = new MultiPoint(new ArrayList<>(shapes));
                    document.add(new BinaryGeoShapeDocValuesField(FIELD_NAME,
                        GeoTestUtils.toDecodedTriangles(geometry), new CentroidCalculator(geometry)));
                    iw.addDocument(document);
                    shapes.clear();
                    distinctHashesPerDoc.clear();
                    document.clear();
                }
            }
            if (shapes.size() != 0) {
                Geometry geometry = new MultiPoint(new ArrayList<>(shapes));
                document.add(new BinaryGeoShapeDocValuesField(FIELD_NAME,
                    GeoTestUtils.toDecodedTriangles(geometry), new CentroidCalculator(geometry)));
                iw.addDocument(document);
            }
        }, geoHashGrid -> {
            assertEquals(expectedCountPerGeoHash.size(), geoHashGrid.getBuckets().size());
            for (GeoGrid.Bucket bucket : geoHashGrid.getBuckets()) {
                assertEquals((long) expectedCountPerGeoHash.get(bucket.getKeyAsString()), bucket.getDocCount());
            }
            assertTrue(AggregationInspectionHelper.hasValue(geoHashGrid));
        }, new GeoShapeFieldMapper.GeoShapeFieldType(), new NoneCircuitBreakerService());
    }

    public void testGeoShapeTrippedCircuitBreaker() throws IOException {
        GeoGridTiler tiler = geoGridTiler();
        int precision = randomIntBetween(1, 9); // does not go until MAX_ZOOM for performance reasons

        @SuppressWarnings("unchecked") Function<Boolean, Geometry> geometryGen = ESTestCase.randomFrom(
            GeometryTestUtils::randomLine,
            GeometryTestUtils::randomPoint,
            GeometryTestUtils::randomPolygon,
            GeometryTestUtils::randomMultiLine,
            GeometryTestUtils::randomMultiPoint
        );
        Geometry geometry = new GeoShapeIndexer(true, "indexer").prepareForIndexing(geometryGen.apply(false));

        // get expected number of tiles to find
        CellIdSource.GeoShapeCellValues values = new CellIdSource.GeoShapeCellValues(null, precision, tiler,
            new NoopCircuitBreaker("test"));
        int numTiles = geoGridTiler().setValues(values, new MultiGeoValues.GeoShapeValue(triangleTreeReader(geometry,
            GeoShapeCoordinateEncoder.INSTANCE)), precision);

        CircuitBreakerService circuitBreakerService = new HierarchyCircuitBreakerService(Settings.EMPTY, new ClusterSettings(Settings.EMPTY,
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        BreakerSettings settings = new BreakerSettings(CircuitBreaker.REQUEST, numTiles * Long.BYTES - 1, 1.0);
        circuitBreakerService.registerBreaker(settings);

        expectThrows(CircuitBreakingException.class, () -> testCase(new MatchAllDocsQuery(), FIELD_NAME, precision, iw -> {
            Document document = new Document();
            document.add(new BinaryGeoShapeDocValuesField(FIELD_NAME,
                GeoTestUtils.toDecodedTriangles(geometry), new CentroidCalculator(geometry)));
            iw.addDocument(document);
            document.clear();
        }, internalGeoGrid -> {}, new GeoShapeFieldMapper.GeoShapeFieldType(), circuitBreakerService));
    }

    private void testCase(Query query, String field, int precision, CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalGeoGrid<T>> verify, MappedFieldType fieldType,
                          CircuitBreakerService circuitBreakerService) throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        GeoGridAggregationBuilder aggregationBuilder = createBuilder("_name").field(field);
        aggregationBuilder.precision(precision);
        fieldType.setHasDocValues(true);
        fieldType.setName(FIELD_NAME);

        try {
            GeoGridAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, circuitBreakerService, fieldType);
            aggregator.preCollection();
            aggregator.preGetSubLeafCollectors();
            indexSearcher.search(query, aggregator);
            aggregator.postCollection();
            verify.accept((InternalGeoGrid<T>) aggregator.buildAggregation(0L));
        } finally {
            indexReader.close();
            directory.close();
        }
    }
}
