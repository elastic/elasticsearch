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
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoBoundingBoxTests;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
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
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

public abstract class GeoGridAggregatorTestCase<T extends InternalGeoGridBucket> extends AggregatorTestCase {

    private static final String FIELD_NAME = "location";
    protected static final double GEOHASH_TOLERANCE = 1E-5D;

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

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return createBuilder("foo").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CoreValuesSourceType.GEOPOINT);
    }

    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), FIELD_NAME, randomPrecision(), null, geoGrid -> {
            assertEquals(0, geoGrid.getBuckets().size());
        }, iw -> {
            // Intentionally not writing any docs
        });
    }

    public void testUnmapped() throws IOException {
        testCase(new MatchAllDocsQuery(), "wrong_field", randomPrecision(), null, geoGrid -> {
            assertEquals(0, geoGrid.getBuckets().size());
        }, iw -> {
            iw.addDocument(Collections.singleton(new LatLonDocValuesField(FIELD_NAME, 10D, 10D)));
        });
    }

    public void testUnmappedMissing() throws IOException {
        GeoGridAggregationBuilder builder = createBuilder("_name")
            .field("wrong_field")
            .missing("53.69437,6.475031");
        testCase(new MatchAllDocsQuery(), randomPrecision(), null, geoGrid -> assertEquals(1, geoGrid.getBuckets().size()),
            iw -> iw.addDocument(Collections.singleton(new LatLonDocValuesField(FIELD_NAME, 10D, 10D))), builder);

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
                double[] latLng = randomLatLng();
                points.add(new LatLonDocValuesField(FIELD_NAME, latLng[0], latLng[1]));
                String hash = hashAsString(latLng[1], latLng[0], precision);
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

    public void testAsSubAgg() throws IOException {
        int precision = randomPrecision();
        Map<String, Map<String, Long>> expectedCountPerTPerGeoHash = new TreeMap<>();
        List<List<IndexableField>> docs = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            String t = randomAlphaOfLength(1);
            double[] latLng = randomLatLng();

            List<IndexableField> doc = new ArrayList<>();
            docs.add(doc);
            doc.add(new LatLonDocValuesField(FIELD_NAME, latLng[0], latLng[1]));
            doc.add(new SortedSetDocValuesField("t", new BytesRef(t)));

            String hash = hashAsString(latLng[1], latLng[0], precision);
            Map<String, Long> expectedCountPerGeoHash = expectedCountPerTPerGeoHash.get(t);
            if (expectedCountPerGeoHash == null) {
                expectedCountPerGeoHash = new TreeMap<>();
                expectedCountPerTPerGeoHash.put(t, expectedCountPerGeoHash);
            }
            expectedCountPerGeoHash.put(hash, expectedCountPerGeoHash.getOrDefault(hash, 0L) + 1);
        }
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex = iw -> iw.addDocuments(docs);
        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("t").field("t")
            .size(expectedCountPerTPerGeoHash.size())
            .subAggregation(createBuilder("gg").field(FIELD_NAME).precision(precision));
        Consumer<StringTerms> verify = (terms) -> {
            Map<String, Map<String, Long>> actual = new TreeMap<>();
            for (StringTerms.Bucket tb: terms.getBuckets()) {
                InternalGeoGrid<?> gg = tb.getAggregations().get("gg");
                Map<String, Long> sub = new TreeMap<>();
                for (InternalGeoGridBucket<?> ggb : gg.getBuckets()) {
                    sub.put(ggb.getKeyAsString(), ggb.getDocCount());
                }
                actual.put(tb.getKeyAsString(), sub);
            }
            assertThat(actual, equalTo(expectedCountPerTPerGeoHash));
        };
        testCase(aggregationBuilder, new MatchAllDocsQuery(), buildIndex, verify, keywordField("t"), geoPointField(FIELD_NAME));
    }

    private double[] randomLatLng() {
        double lat = (180d * randomDouble()) - 90d;
        double lng = (360d * randomDouble()) - 180d;

        // Precision-adjust longitude/latitude to avoid wrong bucket placement
        // Internally, lat/lng get converted to 32 bit integers, loosing some precision.
        // This does not affect geohashing because geohash uses the same algorithm,
        // but it does affect other bucketing algos, thus we need to do the same steps here.
        lng = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lng));
        lat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));

        return new double[] {lat, lng};
    }

    public void testBounds() throws IOException {
        final int numDocs = randomIntBetween(64, 256);
        final GeoGridAggregationBuilder builder = createBuilder("_name");

        expectThrows(IllegalArgumentException.class, () -> builder.precision(-1));
        expectThrows(IllegalArgumentException.class, () -> builder.precision(30));

        // only consider bounding boxes that are at least GEOHASH_TOLERANCE wide and have quantized coordinates
        GeoBoundingBox bbox = randomValueOtherThanMany(
            (b) -> Math.abs(GeoUtils.normalizeLon(b.right()) - GeoUtils.normalizeLon(b.left())) < GEOHASH_TOLERANCE,
            GeoBoundingBoxTests::randomBBox);
        Function<Double, Double> encodeDecodeLat = (lat) -> GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));
        Function<Double, Double> encodeDecodeLon = (lon) -> GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lon));
        bbox.topLeft().reset(encodeDecodeLat.apply(bbox.top()), encodeDecodeLon.apply(bbox.left()));
        bbox.bottomRight().reset(encodeDecodeLat.apply(bbox.bottom()), encodeDecodeLon.apply(bbox.right()));

        int in = 0, out = 0;
        List<LatLonDocValuesField> docs = new ArrayList<>();
        while (in + out < numDocs) {
            if (bbox.left() > bbox.right()) {
                if (randomBoolean()) {
                    double lonWithin = randomBoolean() ?
                        randomDoubleBetween(bbox.left(), 180.0, true)
                        : randomDoubleBetween(-180.0, bbox.right(), true);
                    double latWithin = randomDoubleBetween(bbox.bottom(), bbox.top(), true);
                    in++;
                    docs.add(new LatLonDocValuesField(FIELD_NAME, latWithin, lonWithin));
                } else {
                    double lonOutside = randomDoubleBetween(bbox.left(), bbox.right(), true);
                    double latOutside = randomDoubleBetween(bbox.top(), -90, false);
                    out++;
                    docs.add(new LatLonDocValuesField(FIELD_NAME, latOutside, lonOutside));
                }
            } else {
                if (randomBoolean()) {
                    double lonWithin = randomDoubleBetween(bbox.left(), bbox.right(), true);
                    double latWithin = randomDoubleBetween(bbox.bottom(), bbox.top(), true);
                    in++;
                    docs.add(new LatLonDocValuesField(FIELD_NAME, latWithin, lonWithin));
                } else {
                    double lonOutside = GeoUtils.normalizeLon(randomDoubleBetween(bbox.right(), 180.001, false));
                    double latOutside = GeoUtils.normalizeLat(randomDoubleBetween(bbox.top(), 90.001, false));
                    out++;
                    docs.add(new LatLonDocValuesField(FIELD_NAME, latOutside, lonOutside));
                }
            }

        }

        final long numDocsInBucket = in;
        final int precision = randomPrecision();

        testCase(new MatchAllDocsQuery(), FIELD_NAME, precision, bbox, geoGrid -> {
            assertTrue(AggregationInspectionHelper.hasValue(geoGrid));
            long docCount = 0;
            for (int i = 0; i < geoGrid.getBuckets().size(); i++) {
                docCount += geoGrid.getBuckets().get(i).getDocCount();
            }
            assertThat(docCount, equalTo(numDocsInBucket));
        }, iw -> {
            for (LatLonDocValuesField docField : docs) {
                iw.addDocument(Collections.singletonList(docField));
            }
        });
    }

    private void testCase(Query query, String field, int precision, GeoBoundingBox geoBoundingBox,
                          Consumer<InternalGeoGrid<T>> verify,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex) throws IOException {
        testCase(query, precision, geoBoundingBox, verify, buildIndex, createBuilder("_name").field(field));
    }

    private void testCase(Query query, int precision, GeoBoundingBox geoBoundingBox,
                          Consumer<InternalGeoGrid<T>> verify,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          GeoGridAggregationBuilder aggregationBuilder) throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        aggregationBuilder.precision(precision);
        if (geoBoundingBox != null) {
            aggregationBuilder.setGeoBoundingBox(geoBoundingBox);
            assertThat(aggregationBuilder.geoBoundingBox(), equalTo(geoBoundingBox));
        }

        MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType(FIELD_NAME);

        Aggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(query, aggregator);
        aggregator.postCollection();
        verify.accept((InternalGeoGrid<T>) aggregator.buildTopLevel());

        indexReader.close();
        directory.close();
    }

    @Override
    public void doAssertReducedMultiBucketConsumer(Aggregation agg, MultiBucketConsumerService.MultiBucketConsumer bucketConsumer) {
        /*
         * No-op.
         */
    }
}
