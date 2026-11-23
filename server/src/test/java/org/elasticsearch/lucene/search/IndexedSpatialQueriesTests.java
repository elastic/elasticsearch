/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.document.XYPointField;
import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.lucene.search.IndexedSpatialQueries.Kind;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Builds each spatial query through the Lucene factory Elasticsearch uses and checks how {@link IndexedSpatialQueries} classifies it,
 * so that a Lucene upgrade that renames or replaces one of the package-private classes fails here.
 */
public class IndexedSpatialQueriesTests extends ESTestCase {

    private static final String FIELD = "f";
    private static final Polygon POLYGON = new Polygon(new double[] { 0, 0, 10, 10, 0 }, new double[] { 0, 10, 10, 0, 0 });
    private static final XYPolygon XY_POLYGON = new XYPolygon(new float[] { 0, 10, 10, 0, 0 }, new float[] { 0, 0, 10, 10, 0 });

    public void testClassNamesResolve() throws ClassNotFoundException {
        for (String name : new String[] {
            IndexedSpatialQueries.XY_POINT_IN_GEOMETRY_QUERY,
            IndexedSpatialQueries.LAT_LON_POINT_DISTANCE_QUERY,
            IndexedSpatialQueries.SPATIAL_QUERY,
            IndexedSpatialQueries.SHAPE_DOC_VALUES_QUERY }) {
            assertThat(Class.forName(name, false, Query.class.getClassLoader()).getName(), equalTo(name));
        }
    }

    public void testKinds() {
        assertThat(IndexedSpatialQueries.kindOf(XYPointField.newPolygonQuery(FIELD, XY_POLYGON)), equalTo(Kind.POINT_IN_GEOMETRY));
        assertThat(IndexedSpatialQueries.kindOf(LatLonPoint.newDistanceQuery(FIELD, 1, 2, 1000)), equalTo(Kind.POINT_DISTANCE));
        for (QueryRelation relation : QueryRelation.values()) {
            if (relation != QueryRelation.CONTAINS) {
                assertThat(IndexedSpatialQueries.kindOf(LatLonPoint.newGeometryQuery(FIELD, relation, POLYGON)), equalTo(Kind.SPATIAL));
            }
            assertThat(IndexedSpatialQueries.kindOf(LatLonShape.newGeometryQuery(FIELD, relation, POLYGON)), equalTo(Kind.SPATIAL));
            assertThat(IndexedSpatialQueries.kindOf(LatLonShape.newBoxQuery(FIELD, relation, 0, 10, 0, 10)), equalTo(Kind.SPATIAL));
            assertThat(IndexedSpatialQueries.kindOf(XYShape.newGeometryQuery(FIELD, relation, XY_POLYGON)), equalTo(Kind.SPATIAL));
        }
    }

    public void testNotIndexedSpatial() {
        for (Query query : new Query[] {
            LatLonShape.newSlowDocValuesBoxQuery(FIELD, QueryRelation.INTERSECTS, 0, 10, 0, 10),
            XYShape.newSlowDocValuesBoxQuery(FIELD, QueryRelation.INTERSECTS, 0, 10, 0, 10),
            LatLonDocValuesField.newSlowGeometryQuery(FIELD, QueryRelation.INTERSECTS, POLYGON),
            LatLonDocValuesField.newSlowDistanceQuery(FIELD, 1, 2, 1000),
            LatLonPoint.newDistanceFeatureQuery(FIELD, 1f, 1, 2, 1000),
            LongPoint.newRangeQuery(FIELD, 1, 10),
            new TermQuery(new Term(FIELD, "a")) }) {
            assertThat(query.toString(), IndexedSpatialQueries.kindOf(query), nullValue());
            assertFalse(query.toString(), IndexedSpatialQueries.containsIndexedSpatialQuery(query));
        }
    }

    public void testContainsSeesThroughWrappers() {
        Query spatial = LatLonPoint.newDistanceQuery(FIELD, 1, 2, 1000);
        Query term = new TermQuery(new Term(FIELD, "a"));
        assertTrue(IndexedSpatialQueries.containsIndexedSpatialQuery(spatial));
        assertTrue(IndexedSpatialQueries.containsIndexedSpatialQuery(new ConstantScoreQuery(spatial)));
        assertTrue(
            IndexedSpatialQueries.containsIndexedSpatialQuery(
                new IndexOrDocValuesQuery(spatial, LatLonDocValuesField.newSlowDistanceQuery(FIELD, 1, 2, 1000))
            )
        );
        for (BooleanClause.Occur occur : BooleanClause.Occur.values()) {
            Query bool = new BooleanQuery.Builder().add(term, BooleanClause.Occur.MUST).add(spatial, occur).build();
            assertTrue(occur.toString(), IndexedSpatialQueries.containsIndexedSpatialQuery(bool));
        }
        assertFalse(
            IndexedSpatialQueries.containsIndexedSpatialQuery(
                new BooleanQuery.Builder().add(term, BooleanClause.Occur.MUST).add(term, BooleanClause.Occur.MUST_NOT).build()
            )
        );
    }
}
