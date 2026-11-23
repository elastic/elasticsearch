/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.IntRange;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.LongRange;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.XYPointField;
import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.Circle;
import org.apache.lucene.geo.Point;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.XYCircle;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Builds each charged query through the Lucene factory Elasticsearch uses and checks that {@link ExecutionEstimators} charges it,
 * so that a Lucene upgrade that changes the class a factory returns fails here rather than going uncharged.
 */
public class ExecutionChargeTests extends ESTestCase {

    private static final String FIELD = "f";
    private static final Polygon LAT_LON_POLYGON = new Polygon(new double[] { 0, 0, 10, 10, 0 }, new double[] { 0, 10, 10, 0, 0 });
    private static final XYPolygon XY_POLYGON = new XYPolygon(new float[] { 0, 10, 10, 0, 0 }, new float[] { 0, 0, 10, 10, 0 });

    public void testPointRange() {
        assertCharged(LongPoint.newRangeQuery(FIELD, 1, 10));
    }

    public void testMultiTerm() {
        assertCharged(new WildcardQuery(new Term(FIELD, "a*b")));
    }

    public void testPointInSet() {
        assertCharged(LongPoint.newSetQuery(FIELD, 1, 5, 7));
        assertCharged(IntPoint.newSetQuery(FIELD, 1, 5, 7));
        assertCharged(InetAddressPoint.newSetQuery(FIELD, InetAddresses.forString("10.0.0.1"), InetAddresses.forString("::1")));
    }

    public void testRangeField() {
        long[] min = { 1 };
        long[] max = { 10 };
        assertCharged(LongRange.newIntersectsQuery(FIELD, min, max));
        assertCharged(LongRange.newWithinQuery(FIELD, min, max));
        assertCharged(LongRange.newContainsQuery(FIELD, min, max));
        assertCharged(LongRange.newCrossesQuery(FIELD, min, max));
        assertCharged(IntRange.newIntersectsQuery(FIELD, new int[] { 1 }, new int[] { 10 }));
    }

    public void testRangeFieldIsFoundThroughVisit() {
        assertThat(ExecutionCharge.fieldOf(LongRange.newIntersectsQuery(FIELD, new long[] { 1 }, new long[] { 10 })), equalTo(FIELD));
    }

    public void testLatLonPointDistance() {
        assertCharged(LatLonPoint.newDistanceQuery(FIELD, 1, 2, 1000), LatLonPointDistanceExecutionEstimator.class);
        assertCharged(
            LatLonPoint.newGeometryQuery(FIELD, QueryRelation.INTERSECTS, new Circle(1, 2, 1000)),
            LatLonPointDistanceExecutionEstimator.class
        );
    }

    public void testXYPointInGeometry() {
        assertCharged(XYPointField.newPolygonQuery(FIELD, XY_POLYGON), PointsExecutionEstimator.class);
        assertCharged(XYPointField.newGeometryQuery(FIELD, XY_POLYGON, new XYCircle(1, 2, 3)), PointsExecutionEstimator.class);
    }

    public void testSpatialQueries() {
        for (QueryRelation relation : QueryRelation.values()) {
            if (relation != QueryRelation.CONTAINS) {
                assertCharged(LatLonPoint.newGeometryQuery(FIELD, relation, LAT_LON_POLYGON), SpatialExecutionEstimator.class);
            }
            assertCharged(LatLonShape.newGeometryQuery(FIELD, relation, LAT_LON_POLYGON), SpatialExecutionEstimator.class);
            assertCharged(LatLonShape.newBoxQuery(FIELD, relation, 0, 10, 0, 10), SpatialExecutionEstimator.class);
            assertCharged(XYShape.newGeometryQuery(FIELD, relation, XY_POLYGON), SpatialExecutionEstimator.class);
        }
        assertCharged(LatLonPoint.newPolygonQuery(FIELD, LAT_LON_POLYGON), SpatialExecutionEstimator.class);
        // CONTAINS on points is a ConstantScoreQuery over one LatLonPointQuery per point, which are looked up on their own.
        assertCharged(
            ((BooleanQuery) ((ConstantScoreQuery) LatLonPoint.newGeometryQuery(FIELD, QueryRelation.CONTAINS, new Point(1, 2))).getQuery())
                .clauses()
                .getFirst()
                .query(),
            SpatialExecutionEstimator.class
        );
    }

    public void testShapeDocValuesQueriesAreNotCharged() {
        assertNotCharged(LatLonShape.newSlowDocValuesBoxQuery(FIELD, QueryRelation.INTERSECTS, 0, 10, 0, 10));
        assertNotCharged(XYShape.newSlowDocValuesBoxQuery(FIELD, QueryRelation.INTERSECTS, 0, 10, 0, 10));
        assertNotCharged(LatLonDocValuesField.newSlowGeometryQuery(FIELD, QueryRelation.INTERSECTS, LAT_LON_POLYGON));
    }

    public void testSpatialFieldsAreFoundThroughVisit() {
        for (Query query : new Query[] {
            LatLonPoint.newDistanceQuery(FIELD, 1, 2, 1000),
            LatLonPoint.newPolygonQuery(FIELD, LAT_LON_POLYGON),
            LatLonShape.newGeometryQuery(FIELD, QueryRelation.INTERSECTS, LAT_LON_POLYGON),
            XYPointField.newPolygonQuery(FIELD, XY_POLYGON),
            XYShape.newGeometryQuery(FIELD, QueryRelation.INTERSECTS, XY_POLYGON) }) {
            assertThat(query.toString(), ExecutionCharge.fieldOf(query), equalTo(FIELD));
        }
    }

    public void testBoostAndIndexOrDocValuesAreUnwrapped() {
        Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(FIELD, 1, 10);
        for (Query indexQuery : new Query[] {
            LongPoint.newRangeQuery(FIELD, 1, 10),
            LongPoint.newSetQuery(FIELD, 1, 5, 7),
            LongRange.newIntersectsQuery(FIELD, new long[] { 1 }, new long[] { 10 }) }) {
            assertCharged(new BoostQuery(indexQuery, 2f));
            assertCharged(new IndexOrDocValuesQuery(indexQuery, dvQuery));
            assertCharged(new BoostQuery(new IndexOrDocValuesQuery(indexQuery, dvQuery), 2f));
        }
    }

    public void testNotCharged() {
        assertNotCharged(new TermQuery(new Term(FIELD, "a")));
        assertNotCharged(MatchAllDocsQuery.INSTANCE);
        assertNotCharged(SortedNumericDocValuesField.newSlowSetQuery(FIELD, 1, 5));
        // ConstantScoreQuery creates its child's weight through the searcher, which looks the child up on its own.
        assertNotCharged(new ConstantScoreQuery(LongPoint.newSetQuery(FIELD, 1, 5)));
    }

    private static void assertCharged(Query query) {
        assertThat("no charge for " + query, ExecutionCharge.forQuery(query), notNullValue());
    }

    private static void assertCharged(Query query, Class<? extends LeafExecutionEstimator> estimatorClass) {
        ExecutionCharge charge = ExecutionCharge.forQuery(query);
        assertThat("no charge for " + query, charge, notNullValue());
        assertThat(query.toString(), charge.estimator(), instanceOf(estimatorClass));
    }

    private static void assertNotCharged(Query query) {
        assertThat("unexpected charge for " + query, ExecutionCharge.forQuery(query), nullValue());
    }
}
