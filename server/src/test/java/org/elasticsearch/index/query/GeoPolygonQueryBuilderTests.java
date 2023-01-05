/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.core.Strings;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;

public class GeoPolygonQueryBuilderTests extends AbstractQueryTestCase<GeoPolygonQueryBuilder> {
    @Override
    protected GeoPolygonQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(GEO_POINT_FIELD_NAME, GEO_POINT_ALIAS_FIELD_NAME);
        List<GeoPoint> polygon = randomPolygon();
        GeoPolygonQueryBuilder builder = new GeoPolygonQueryBuilder(fieldName, polygon);
        if (randomBoolean()) {
            builder.setValidationMethod(randomFrom(GeoValidationMethod.values()));
        }

        if (randomBoolean()) {
            builder.ignoreUnmapped(randomBoolean());
        }

        return builder;
    }

    @Override
    protected void doAssertLuceneQuery(GeoPolygonQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        MappedFieldType fieldType = context.getFieldType(queryBuilder.fieldName());
        if (fieldType == null) {
            assertTrue("Found no indexed geo query.", query instanceof MatchNoDocsQuery);
        } else { // TODO: Test case when there are no docValues
            Query indexQuery = ((IndexOrDocValuesQuery) query).getIndexQuery();
            String expectedFieldName = expectedFieldName(queryBuilder.fieldName());
            List<GeoPoint> points = queryBuilder.points();
            double[] lats = new double[points.size()];
            double[] lons = new double[points.size()];
            for (int i = 0; i < points.size(); i++) {
                lats[i] = points.get(i).getLat();
                lons[i] = points.get(i).getLon();
            }
            org.apache.lucene.geo.Polygon polygon = new org.apache.lucene.geo.Polygon(lats, lons);
            assertEquals(LatLonPoint.newPolygonQuery(expectedFieldName, polygon), indexQuery);
            Query dvQuery = ((IndexOrDocValuesQuery) query).getRandomAccessQuery();
            assertEquals(LatLonDocValuesField.newSlowPolygonQuery(expectedFieldName, polygon), dvQuery);
        }
    }

    @Override
    public void testUnknownField() throws IOException {
        super.testUnknownField();
        assertDeprecationWarning();
    }

    @Override
    public void testUnknownObjectException() throws IOException {
        super.testUnknownObjectException();
        assertDeprecationWarning();
    }

    @Override
    public void testFromXContent() throws IOException {
        super.testFromXContent();
        assertDeprecationWarning();
    }

    @Override
    public void testValidOutput() throws IOException {
        super.testValidOutput();
        assertDeprecationWarning();
    }

    private void assertDeprecationWarning() {
        assertWarnings("Deprecated field [geo_polygon] used, replaced by [[geo_shape] query where polygons are defined in geojson or wkt]");
    }

    private static List<GeoPoint> randomPolygon() {
        LinearRing linearRing = GeometryTestUtils.randomPolygon(false).getPolygon();
        List<GeoPoint> polygonPoints = new ArrayList<>(linearRing.length());
        for (int i = 0; i < linearRing.length(); i++) {
            polygonPoints.add(new GeoPoint(linearRing.getLat(i), linearRing.getLon(i)));
        }
        return polygonPoints;
    }

    public void testNullFieldName() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GeoPolygonQueryBuilder(null, randomPolygon()));
        assertEquals("fieldName must not be null", e.getMessage());
    }

    public void testEmptyPolygon() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new GeoPolygonQueryBuilder(GEO_POINT_FIELD_NAME, Collections.emptyList())
        );
        assertEquals("polygon must not be null or empty", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new GeoPolygonQueryBuilder(GEO_POINT_FIELD_NAME, null));
        assertEquals("polygon must not be null or empty", e.getMessage());
    }

    public void testInvalidClosedPolygon() {
        List<GeoPoint> points = new ArrayList<>();
        points.add(new GeoPoint(0, 90));
        points.add(new GeoPoint(90, 90));
        points.add(new GeoPoint(0, 90));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new GeoPolygonQueryBuilder(GEO_POINT_FIELD_NAME, points)
        );
        assertEquals("too few points defined for geo_polygon query", e.getMessage());
    }

    public void testInvalidOpenPolygon() {
        List<GeoPoint> points = new ArrayList<>();
        points.add(new GeoPoint(0, 90));
        points.add(new GeoPoint(90, 90));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new GeoPolygonQueryBuilder(GEO_POINT_FIELD_NAME, points)
        );
        assertEquals("too few points defined for geo_polygon query", e.getMessage());
    }

    public void testParsingAndToQueryParsingExceptions() throws IOException {
        String[] brokenFiles = new String[] {
            "/org/elasticsearch/index/query/geo_polygon_exception_1.json",
            "/org/elasticsearch/index/query/geo_polygon_exception_2.json",
            "/org/elasticsearch/index/query/geo_polygon_exception_3.json",
            "/org/elasticsearch/index/query/geo_polygon_exception_4.json",
            "/org/elasticsearch/index/query/geo_polygon_exception_5.json" };
        for (String brokenFile : brokenFiles) {
            String query = copyToStringFromClasspath(brokenFile);
            expectThrows(ParsingException.class, () -> parseQuery(query));
        }
        assertDeprecationWarning();
    }

    public void testParsingAndToQuery1() throws IOException {
        String query = Strings.format("""
            {
              "geo_polygon": {
                "%s": {
                  "points": [
                    [ -70, 40 ],
                    [ -80, 30 ],
                    [ -90, 20 ]
                  ]
                }
              }
            }""", GEO_POINT_FIELD_NAME);
        assertGeoPolygonQuery(query);
        assertDeprecationWarning();
    }

    public void testParsingAndToQuery2() throws IOException {
        String query = Strings.format("""
            {
              "geo_polygon": {
                "%s": {
                  "points": [ { "lat": 40, "lon": -70 }, { "lat": 30, "lon": -80 }, { "lat": 20, "lon": -90 } ]
                }
              }
            }""", GEO_POINT_FIELD_NAME);
        assertGeoPolygonQuery(query);
        assertDeprecationWarning();
    }

    public void testParsingAndToQuery3() throws IOException {
        String query = Strings.format("""
            {
              "geo_polygon": {
                "%s": {
                  "points": [ "40, -70", "30, -80", "20, -90" ]
                }
              }
            }""", GEO_POINT_FIELD_NAME);
        assertGeoPolygonQuery(query);
        assertDeprecationWarning();
    }

    public void testParsingAndToQuery4() throws IOException {
        String query = Strings.format("""
            {
              "geo_polygon": {
                "%s": {
                  "points": [ "drn5x1g8cu2y", "30, -80", "20, -90" ]
                }
              }
            }""", GEO_POINT_FIELD_NAME);
        assertGeoPolygonQuery(query);
        assertDeprecationWarning();
    }

    private void assertGeoPolygonQuery(String query) throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        GeoPolygonQueryBuilder queryBuilder = (GeoPolygonQueryBuilder) parseQuery(query);
        doAssertLuceneQuery(queryBuilder, queryBuilder.toQuery(context), context);
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "geo_polygon" : {
                "person.location" : {
                  "points" : [ [ -70.0, 40.0 ], [ -80.0, 30.0 ], [ -90.0, 20.0 ], [ -70.0, 40.0 ] ]
                },
                "validation_method" : "STRICT",
                "ignore_unmapped" : false,
                "boost" : 1.0
              }
            }""";
        GeoPolygonQueryBuilder parsed = (GeoPolygonQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, 4, parsed.points().size());
        assertDeprecationWarning();
    }

    public void testIgnoreUnmapped() throws IOException {
        List<GeoPoint> polygon = randomPolygon();
        final GeoPolygonQueryBuilder queryBuilder = new GeoPolygonQueryBuilder("unmapped", polygon);
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(createSearchExecutionContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final GeoPolygonQueryBuilder failingQueryBuilder = new GeoPolygonQueryBuilder("unmapped", polygon);
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(createSearchExecutionContext()));
        assertThat(e.getMessage(), containsString("failed to find geo_point field [unmapped]"));
    }

    public void testPointValidation() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        String queryInvalidLat = Strings.format("""
            {
              "geo_polygon": {
                "%s": {
                  "points": [
                    [ -70, 140 ],
                    [ -80, 30 ],
                    [ -90, 20 ]
                  ]
                }
              }
            }""", GEO_POINT_FIELD_NAME);

        QueryShardException e1 = expectThrows(QueryShardException.class, () -> parseQuery(queryInvalidLat).toQuery(context));
        assertThat(e1.getMessage(), containsString("illegal latitude value [140.0] for [geo_polygon]"));

        String queryInvalidLon = Strings.format("""
            {
              "geo_polygon": {
                "%s": {
                  "points": [
                    [ -70, 40 ],
                    [ -80, 30 ],
                    [ -190, 20 ]
                  ]
                }
              }
            }""", GEO_POINT_FIELD_NAME);

        QueryShardException e2 = expectThrows(QueryShardException.class, () -> parseQuery(queryInvalidLon).toQuery(context));
        assertThat(e2.getMessage(), containsString("illegal longitude value [-190.0] for [geo_polygon]"));
        assertDeprecationWarning();
    }
}
