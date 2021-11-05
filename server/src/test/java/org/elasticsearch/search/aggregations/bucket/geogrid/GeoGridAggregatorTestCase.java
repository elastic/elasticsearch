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
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
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
            iw -> { iw.addDocument(Collections.singleton(new LatLonDocValuesField(FIELD_NAME, 10D, 10D))); }
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
            doc.add(new Field("t", new BytesRef(t), KeywordFieldMapper.Defaults.FIELD_TYPE));

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
            for (StringTerms.Bucket tb : terms.getBuckets()) {
                InternalGeoGrid<?> gg = tb.getAggregations().get("gg");
                Map<String, Long> sub = new TreeMap<>();
                for (InternalGeoGridBucket ggb : gg.getBuckets()) {
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

        return new double[] { lat, lng };
    }

    public void testBounds() throws IOException {
        final int numDocs = randomIntBetween(64, 256);
        final GeoGridAggregationBuilder builder = createBuilder("_name");

        expectThrows(IllegalArgumentException.class, () -> builder.precision(-1));
        expectThrows(IllegalArgumentException.class, () -> builder.precision(30));

        GeoBoundingBox bbox = randomBBox();
        final double boundsTop = bbox.top();
        final double boundsBottom = bbox.bottom();
        final double boundsWestLeft;
        final double boundsWestRight;
        final double boundsEastLeft;
        final double boundsEastRight;
        final boolean crossesDateline;
        if (bbox.right() < bbox.left()) {
            boundsWestLeft = -180;
            boundsWestRight = bbox.right();
            boundsEastLeft = bbox.left();
            boundsEastRight = 180;
            crossesDateline = true;
        } else { // only set east bounds
            boundsEastLeft = bbox.left();
            boundsEastRight = bbox.right();
            boundsWestLeft = 0;
            boundsWestRight = 0;
            crossesDateline = false;
        }

        Function<Double, Double> encodeDecodeLat = (lat) -> GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(lat));
        Function<Double, Double> encodeDecodeLon = (lon) -> GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(lon));
        final int precision = randomPrecision();
        int in = 0;
        List<LatLonDocValuesField> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            Point p = randomPoint();
            double x = encodeDecodeLon.apply(p.getLon());
            double y = encodeDecodeLat.apply(p.getLat());
            Rectangle pointTile = getTile(x, y, precision);
            boolean intersectsBounds = boundsTop > pointTile.getMinY()
                && boundsBottom < pointTile.getMaxY()
                && (boundsEastLeft < pointTile.getMaxX() && boundsEastRight > pointTile.getMinX()
                    || (crossesDateline && boundsWestLeft < pointTile.getMaxX() && boundsWestRight > pointTile.getMinX()));
            if (intersectsBounds) {
                in++;
            }
            docs.add(new LatLonDocValuesField(FIELD_NAME, p.getLat(), p.getLon()));
        }

        final long numDocsInBucket = in;
        testCase(new MatchAllDocsQuery(), FIELD_NAME, precision, bbox, geoGrid -> {
            if (numDocsInBucket > 0) {
                assertTrue(AggregationInspectionHelper.hasValue(geoGrid));
                long docCount = 0;
                for (int i = 0; i < geoGrid.getBuckets().size(); i++) {
                    docCount += geoGrid.getBuckets().get(i).getDocCount();
                }
                assertThat(docCount, equalTo(numDocsInBucket));
            } else {
                assertFalse(AggregationInspectionHelper.hasValue(geoGrid));
            }
        }, iw -> {
            for (LatLonDocValuesField docField : docs) {
                iw.addDocument(Collections.singletonList(docField));
            }
        });
    }

    private void testCase(
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
        @SuppressWarnings("unchecked")
        InternalGeoGrid<T> topLevel = (InternalGeoGrid<T>) aggregator.buildTopLevel();
        verify.accept(topLevel);

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
