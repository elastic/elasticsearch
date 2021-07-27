/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGridBucket;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.mapper.BinaryGeoShapeDocValuesField;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeWithDocValuesFieldMapper.GeoShapeWithDocValuesFieldType;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.spatial.util.GeoTestUtils.binaryGeoShapeDocValuesField;
import static org.elasticsearch.xpack.spatial.util.GeoTestUtils.geoShapeValue;
import static org.hamcrest.Matchers.equalTo;

public abstract class GeoShapeGeoGridTestCase<T extends InternalGeoGridBucket<T>> extends AggregatorTestCase {
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

    /**
     * Create a new named {@link GeoGridAggregationBuilder}-derived builder
     */
    protected abstract GeoGridAggregationBuilder createBuilder(String name);

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new LocalStateSpatialPlugin());
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(GeoShapeValuesSourceType.instance(), CoreValuesSourceType.GEOPOINT);
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return createBuilder("foo").field(fieldName);
    }

    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), FIELD_NAME, randomPrecision(), null, iw -> {
            // Intentionally not writing any docs
        }, geoGrid -> {
            assertEquals(0, geoGrid.getBuckets().size());
        });
    }

    public void testUnmapped() throws IOException {
        testCase(new MatchAllDocsQuery(), "wrong_field", randomPrecision(), null, iw -> {
            iw.addDocument(
                Collections.singleton(GeoTestUtils.binaryGeoShapeDocValuesField(FIELD_NAME, new Point(10D, 10D)))
            );
        }, geoGrid -> {
            assertEquals(0, geoGrid.getBuckets().size());
        });
    }


    public void testUnmappedMissingGeoShape() throws IOException {
        // default value type for agg is GEOPOINT, so missing value is parsed as a GEOPOINT
        GeoGridAggregationBuilder builder = createBuilder("_name")
            .field("wrong_field")
            .missing("-34.0,53.4");
        testCase(new MatchAllDocsQuery(), 1, null,
            iw -> {
                iw.addDocument(
                    Collections.singleton(GeoTestUtils.binaryGeoShapeDocValuesField(FIELD_NAME, new Point(10D, 10D)))
                );
            },
            geoGrid -> assertEquals(1, geoGrid.getBuckets().size()), builder);
    }

    public void testMappedMissingGeoShape() throws IOException {
        GeoGridAggregationBuilder builder = createBuilder("_name")
            .field(FIELD_NAME)
            .missing("LINESTRING (30 10, 10 30, 40 40)");
        testCase(new MatchAllDocsQuery(), 1, null,
            iw -> {
                iw.addDocument(Collections.singleton(new SortedSetDocValuesField("string", new BytesRef("a"))));
            },
            geoGrid -> assertEquals(1, geoGrid.getBuckets().size()), builder);
    }

    public void testGeoShapeBounds() throws IOException {
        final int precision = randomPrecision();
        final int numDocs = randomIntBetween(100, 200);
        int numDocsWithin = 0;
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

        List<BinaryGeoShapeDocValuesField> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            Point p;
            p = randomPoint();
            double x = GeoTestUtils.encodeDecodeLon(p.getX());
            double y = GeoTestUtils.encodeDecodeLat(p.getY());
            Rectangle pointTile = getTile(x, y, precision);

            GeoShapeValues.GeoShapeValue value = geoShapeValue(p);
            GeoRelation tileRelation =  value.relate(pointTile);
            boolean intersectsBounds = boundsTop > pointTile.getMinY() && boundsBottom < pointTile.getMaxY()
                && (boundsEastLeft < pointTile.getMaxX() && boundsEastRight > pointTile.getMinX()
                || (crossesDateline && boundsWestLeft < pointTile.getMaxX() && boundsWestRight > pointTile.getMinX()));
            if (tileRelation != GeoRelation.QUERY_DISJOINT && intersectsBounds) {
                numDocsWithin += 1;
            }

            docs.add(binaryGeoShapeDocValuesField(FIELD_NAME, p));
        }

        final long numDocsInBucket = numDocsWithin;

        testCase(new MatchAllDocsQuery(), FIELD_NAME, precision, bbox, iw -> {
                for (BinaryGeoShapeDocValuesField docField : docs) {
                    iw.addDocument(Collections.singletonList(docField));
                }
            },
            geoGrid -> {
                assertThat(AggregationInspectionHelper.hasValue(geoGrid), equalTo(numDocsInBucket > 0));
                long docCount = 0;
                for (int i = 0; i < geoGrid.getBuckets().size(); i++) {
                    docCount += geoGrid.getBuckets().get(i).getDocCount();
                }
                assertThat(docCount, equalTo(numDocsInBucket));
            });
    }

    public void testGeoShapeWithSeveralDocs() throws IOException {
        int precision = randomIntBetween(1, 4);
        int numShapes = randomIntBetween(8, 128);
        Map<String, Integer> expectedCountPerGeoHash = new HashMap<>();
        testCase(new MatchAllDocsQuery(), FIELD_NAME, precision, null, iw -> {
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
                    document.add(binaryGeoShapeDocValuesField(FIELD_NAME, geometry));
                    iw.addDocument(document);
                    shapes.clear();
                    distinctHashesPerDoc.clear();
                    document.clear();
                }
            }
            if (shapes.size() != 0) {
                Geometry geometry = new MultiPoint(new ArrayList<>(shapes));
                document.add(binaryGeoShapeDocValuesField(FIELD_NAME, geometry));
                iw.addDocument(document);
            }
        }, geoHashGrid -> {
            assertEquals(expectedCountPerGeoHash.size(), geoHashGrid.getBuckets().size());
            for (GeoGrid.Bucket bucket : geoHashGrid.getBuckets()) {
                assertEquals((long) expectedCountPerGeoHash.get(bucket.getKeyAsString()), bucket.getDocCount());
            }
            assertTrue(AggregationInspectionHelper.hasValue(geoHashGrid));
        });
    }

    private void testCase(Query query, String field, int precision, GeoBoundingBox geoBoundingBox,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalGeoGrid<T>> verify) throws IOException {
        testCase(query, precision, geoBoundingBox, buildIndex, verify, createBuilder("_name").field(field));
    }

    @SuppressWarnings("unchecked")
    private void testCase(Query query, int precision, GeoBoundingBox geoBoundingBox,
                          CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
                          Consumer<InternalGeoGrid<T>> verify,
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

        MappedFieldType fieldType
            = new GeoShapeWithDocValuesFieldType(FIELD_NAME, true, true, Orientation.RIGHT, null, new SetOnce<>(), Collections.emptyMap());

        Aggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(query, aggregator);
        aggregator.postCollection();

        verify.accept((InternalGeoGrid<T>) aggregator.buildTopLevel());

        indexReader.close();
        directory.close();
    }
}
