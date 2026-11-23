/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.document.XYPointField;
import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.lucene.search.cost.DocIdSetBuilderCostEstimator;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.search.internal.BreakerWeightTestUtils.assertChargeIsCeilingOnRetainedSet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Checks that what {@link ExecutionCharge} charges for the Lucene geo queries is a ceiling on the doc id set each leaf's
 * scorer keeps, for every relation, on single- and multivalued fields.
 */
public class SpatialExecutionEstimatorTests extends ESTestCase {

    private static final String GEO_POINT = "gp";
    private static final String GEO_SHAPE = "gs";
    private static final String XY_POINT = "xp";
    private static final String XY_SHAPE = "xs";

    private Directory directory;
    private DirectoryReader reader;

    @Before
    public void indexRandomGeometries() throws IOException {
        directory = newDirectory();
        boolean multiValued = randomBoolean();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null))) {
            int numDocs = between(1000, 10_000);
            for (int docId = 0; docId < numDocs; docId++) {
                Document doc = new Document();
                int values = multiValued ? between(1, 3) : 1;
                for (int v = 0; v < values; v++) {
                    double lat = randomDoubleBetween(-10, 10, true);
                    double lon = randomDoubleBetween(-10, 10, true);
                    doc.add(new LatLonPoint(GEO_POINT, lat, lon));
                    doc.add(new XYPointField(XY_POINT, (float) lon, (float) lat));
                    for (Field field : LatLonShape.createIndexableFields(GEO_SHAPE, square(lat, lon, 0.01))) {
                        doc.add(field);
                    }
                    for (Field field : XYShape.createIndexableFields(XY_SHAPE, xySquare((float) lon, (float) lat, 0.01f))) {
                        doc.add(field);
                    }
                }
                writer.addDocument(doc);
            }
            if (randomBoolean()) {
                writer.forceMerge(1);
            }
        }
        reader = DirectoryReader.open(directory);
    }

    @After
    public void closeReader() throws IOException {
        IOUtils.close(reader, directory);
    }

    public void testChargeIsCeilingOnRetainedSet() throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        for (int iter = 0; iter < 10; iter++) {
            double lat = randomDoubleBetween(-12, 12, true);
            double lon = randomDoubleBetween(-12, 12, true);
            double halfSide = randomDoubleBetween(0.01, 15, true);
            assertChargeIsCeilingOnRetainedSet(searcher, LatLonPoint.newDistanceQuery(GEO_POINT, lat, lon, halfSide * 111_000));
            assertChargeIsCeilingOnRetainedSet(
                searcher,
                XYPointField.newPolygonQuery(XY_POINT, xySquare((float) lon, (float) lat, (float) halfSide))
            );
            for (QueryRelation relation : QueryRelation.values()) {
                if (relation != QueryRelation.CONTAINS) {
                    assertChargeIsCeilingOnRetainedSet(
                        searcher,
                        LatLonPoint.newGeometryQuery(GEO_POINT, relation, square(lat, lon, halfSide))
                    );
                }
                // A shape contains a point inside it, which keeps the CONTAINS results non-empty.
                Polygon polygon = relation == QueryRelation.CONTAINS ? square(lat, lon, 0.001) : square(lat, lon, halfSide);
                assertChargeIsCeilingOnRetainedSet(searcher, LatLonShape.newGeometryQuery(GEO_SHAPE, relation, polygon));
                assertChargeIsCeilingOnRetainedSet(
                    searcher,
                    XYShape.newGeometryQuery(XY_SHAPE, relation, xySquare((float) lon, (float) lat, 0.001f))
                );
            }
        }
    }

    /** The reservation also covers the {@code DocIdSetBuilder} that the {@code INTERSECTS} and single-valued paths collect into. */
    public void testPeakCoversDenseDocIdSetBuilder() {
        for (int maxDoc : new int[] { 1, 63, 64, 127, 128, 500, 850, 1000, 4095, 8192, 65_536, 1 << 20, Integer.MAX_VALUE }) {
            assertPeakCoversBuilder(maxDoc);
        }
        for (int i = 0; i < 1000; i++) {
            assertPeakCoversBuilder(between(1, Integer.MAX_VALUE));
        }
    }

    private static void assertPeakCoversBuilder(int maxDoc) {
        assertThat(
            "maxDoc=" + maxDoc,
            SpatialExecutionEstimator.peakBytes(maxDoc),
            greaterThanOrEqualTo(new DocIdSetBuilderCostEstimator(Long.MAX_VALUE, maxDoc).estimate())
        );
    }

    /** Every document has one point and the circle covers most of them, so the distance query builds the inverse bit set. */
    public void testDistanceChargesInverseBitSet() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(null))) {
                for (int docId = 0; docId < 2000; docId++) {
                    Document doc = new Document();
                    doc.add(new LatLonPoint(GEO_POINT, randomDoubleBetween(-1, 1, true), randomDoubleBetween(-1, 1, true)));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader singleValued = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(singleValued);
                Query query = LatLonPoint.newDistanceQuery(GEO_POINT, 0, 0, 1_000_000);
                Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);
                LeafReaderContext ctx = singleValued.leaves().getFirst();
                long charged = ExecutionCharge.forQuery(query).estimator().bytesForLeaf(ctx, weight.scorerSupplier(ctx), Long.MAX_VALUE);
                assertThat(charged, equalTo(PointsExecutionEstimator.bitSetBytes(ctx.reader().maxDoc())));
            }
        }
    }

    private static Polygon square(double lat, double lon, double halfSide) {
        double minLat = Math.max(-90, lat - halfSide);
        double maxLat = Math.min(90, lat + halfSide);
        double minLon = Math.max(-180, lon - halfSide);
        double maxLon = Math.min(180, lon + halfSide);
        return new Polygon(
            new double[] { minLat, minLat, maxLat, maxLat, minLat },
            new double[] { minLon, maxLon, maxLon, minLon, minLon }
        );
    }

    private static XYPolygon xySquare(float x, float y, float halfSide) {
        return new XYPolygon(
            new float[] { x - halfSide, x + halfSide, x + halfSide, x - halfSide, x - halfSide },
            new float[] { y - halfSide, y - halfSide, y + halfSide, y + halfSide, y - halfSide }
        );
    }
}
