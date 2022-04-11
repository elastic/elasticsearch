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
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;

public class GeoDistanceQueryBuilderTests extends AbstractQueryTestCase<GeoDistanceQueryBuilder> {

    @Override
    protected GeoDistanceQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(GEO_POINT_FIELD_NAME, GEO_POINT_ALIAS_FIELD_NAME, GEO_SHAPE_FIELD_NAME);
        GeoDistanceQueryBuilder qb = new GeoDistanceQueryBuilder(fieldName);
        String distance = "" + randomDouble();
        if (randomBoolean()) {
            DistanceUnit unit = randomFrom(DistanceUnit.values());
            distance = distance + unit.toString();
        }
        int selector = randomIntBetween(0, 2);
        switch (selector) {
            case 0 -> qb.distance(randomDouble(), randomFrom(DistanceUnit.values()));
            case 1 -> qb.distance(distance, randomFrom(DistanceUnit.values()));
            case 2 -> qb.distance(distance);
        }

        qb.point(new GeoPoint(GeometryTestUtils.randomLat(), GeometryTestUtils.randomLon()));

        if (randomBoolean()) {
            qb.setValidationMethod(randomFrom(GeoValidationMethod.values()));
        }

        if (randomBoolean()) {
            qb.geoDistance(randomFrom(GeoDistance.values()));
        }

        if (randomBoolean()) {
            qb.ignoreUnmapped(randomBoolean());
        }
        return qb;
    }

    public void testIllegalValues() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GeoDistanceQueryBuilder(""));
        assertEquals("fieldName must not be null or empty", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new GeoDistanceQueryBuilder((String) null));
        assertEquals("fieldName must not be null or empty", e.getMessage());

        GeoDistanceQueryBuilder query = new GeoDistanceQueryBuilder("fieldName");
        e = expectThrows(IllegalArgumentException.class, () -> query.distance(""));
        assertEquals("distance must not be null or empty", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> query.distance(null));
        assertEquals("distance must not be null or empty", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> query.distance("", DistanceUnit.DEFAULT));
        assertEquals("distance must not be null or empty", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> query.distance(null, DistanceUnit.DEFAULT));
        assertEquals("distance must not be null or empty", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> query.distance("1", null));
        assertEquals("distance unit must not be null", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> query.distance(1, null));
        assertEquals("distance unit must not be null", e.getMessage());

        e = expectThrows(
            IllegalArgumentException.class,
            () -> query.distance(randomIntBetween(Integer.MIN_VALUE, 0), DistanceUnit.DEFAULT)
        );
        assertEquals("distance must be greater than zero", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> query.geohash(null));
        assertEquals("geohash must not be null or empty", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> query.geohash(""));
        assertEquals("geohash must not be null or empty", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> query.geoDistance(null));
        assertEquals("geoDistance must not be null", e.getMessage());
    }

    /**
     * Overridden here to ensure the test is only run if at least one type is
     * present in the mappings. Geo queries do not execute if the field is not
     * explicitly mapped
     */
    @Override
    public void testToQuery() throws IOException {
        super.testToQuery();
    }

    @Override
    protected void doAssertLuceneQuery(GeoDistanceQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        final MappedFieldType fieldType = context.getFieldType(queryBuilder.fieldName());
        if (fieldType == null) {
            assertTrue("Found no indexed geo query.", query instanceof MatchNoDocsQuery);
        } else if (fieldType instanceof GeoPointFieldMapper.GeoPointFieldType) {
            Query indexQuery = ((IndexOrDocValuesQuery) query).getIndexQuery();

            String expectedFieldName = expectedFieldName(queryBuilder.fieldName());
            assertEquals(
                LatLonPoint.newDistanceQuery(
                    expectedFieldName,
                    queryBuilder.point().lat(),
                    queryBuilder.point().lon(),
                    queryBuilder.distance()
                ),
                indexQuery
            );
            Query dvQuery = ((IndexOrDocValuesQuery) query).getRandomAccessQuery();
            assertEquals(
                LatLonDocValuesField.newSlowDistanceQuery(
                    expectedFieldName,
                    queryBuilder.point().lat(),
                    queryBuilder.point().lon(),
                    queryBuilder.distance()
                ),
                dvQuery
            );
        } else {
            assertEquals(GeoShapeFieldMapper.GeoShapeFieldType.class, fieldType.getClass());
        }
    }

    public void testParsingAndToQuery1() throws IOException {
        String query = """
            {
                "geo_distance":{
                    "distance":"12mi",
                    "%s":{
                        "lat":40,
                        "lon":-70
                    }
                }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        assertGeoDistanceRangeQuery(query, 40, -70, 12, DistanceUnit.MILES);
    }

    public void testParsingAndToQuery2() throws IOException {
        String query = """
            {
                "geo_distance":{
                    "distance":"12mi",
                    "%s":[-70, 40]
                }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        assertGeoDistanceRangeQuery(query, 40, -70, 12, DistanceUnit.MILES);
    }

    public void testParsingAndToQuery3() throws IOException {
        String query = """
            {
                "geo_distance":{
                    "distance":"12mi",
                    "%s":"40, -70"
                }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        assertGeoDistanceRangeQuery(query, 40, -70, 12, DistanceUnit.MILES);
    }

    public void testParsingAndToQuery4() throws IOException {
        String query = """
            {
                "geo_distance":{
                    "distance":"12mi",
                    "%s":"drn5x1g8cu2y"
                }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        GeoPoint geoPoint = GeoPoint.fromGeohash("drn5x1g8cu2y");
        assertGeoDistanceRangeQuery(query, geoPoint.getLat(), geoPoint.getLon(), 12, DistanceUnit.MILES);
    }

    public void testParsingAndToQuery5() throws IOException {
        String query = """
            {
                "geo_distance":{
                    "distance":12,
                    "unit":"mi",
                    "%s":{
                        "lat":40,
                        "lon":-70
                    }
                }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        assertGeoDistanceRangeQuery(query, 40, -70, 12, DistanceUnit.MILES);
    }

    public void testParsingAndToQuery6() throws IOException {
        String query = """
            {
                "geo_distance":{
                    "distance":"12",
                    "unit":"mi",
                    "%s":{
                        "lat":40,
                        "lon":-70
                    }
                }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        assertGeoDistanceRangeQuery(query, 40, -70, 12, DistanceUnit.MILES);
    }

    public void testParsingAndToQuery7() throws IOException {
        String query = """
            {
              "geo_distance":{
                  "distance":"19.312128",
                  "%s":{
                      "lat":40,
                      "lon":-70
                  }
              }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        assertGeoDistanceRangeQuery(query, 40, -70, 19.312128, DistanceUnit.DEFAULT);
    }

    public void testParsingAndToQuery8() throws IOException {
        String query = """
            {
                "geo_distance":{
                    "distance":19.312128,
                    "%s":{
                        "lat":40,
                        "lon":-70
                    }
                }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        assertGeoDistanceRangeQuery(query, 40, -70, 19.312128, DistanceUnit.DEFAULT);
    }

    public void testParsingAndToQuery9() throws IOException {
        String query = """
            {
                "geo_distance":{
                    "distance":"19.312128",
                    "unit":"km",
                    "%s":{
                        "lat":40,
                        "lon":-70
                    }
                }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        assertGeoDistanceRangeQuery(query, 40, -70, 19.312128, DistanceUnit.KILOMETERS);
    }

    public void testParsingAndToQuery10() throws IOException {
        String query = """
            {
                "geo_distance":{
                    "distance":19.312128,
                    "unit":"km",
                    "%s":{
                        "lat":40,
                        "lon":-70
                    }
                }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        assertGeoDistanceRangeQuery(query, 40, -70, 19.312128, DistanceUnit.KILOMETERS);
    }

    public void testParsingAndToQuery11() throws IOException {
        String query = """
            {
                "geo_distance":{
                    "distance":"19.312128km",
                    "%s":{
                        "lat":40,
                        "lon":-70
                    }
                }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        assertGeoDistanceRangeQuery(query, 40, -70, 19.312128, DistanceUnit.KILOMETERS);
    }

    public void testParsingAndToQuery12() throws IOException {
        String query = """
            {
                "geo_distance":{
                    "distance":"12mi",
                    "unit":"km",
                    "%s":{
                        "lat":40,
                        "lon":-70
                    }
                }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        assertGeoDistanceRangeQuery(query, 40, -70, 12, DistanceUnit.MILES);
    }

    private void assertGeoDistanceRangeQuery(String query, double lat, double lon, double distance, DistanceUnit distanceUnit)
        throws IOException {
        Query parsedQuery = parseQuery(query).toQuery(createSearchExecutionContext());
        // The parsedQuery contains IndexOrDocValuesQuery, which wraps LatLonPointDistanceQuery which in turn has default visibility,
        // so we cannot access its fields directly to check and have to use toString() here instead.
        assertEquals(parsedQuery.toString(), "mapped_geo_point:" + lat + "," + lon + " +/- " + distanceUnit.toMeters(distance) + " meters");
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "geo_distance" : {
                "pin.location" : [ -70.0, 40.0 ],
                "distance" : 12000.0,
                "distance_type" : "arc",
                "validation_method" : "STRICT",
                "ignore_unmapped" : false,
                "boost" : 1.0
              }
            }""";
        GeoDistanceQueryBuilder parsed = (GeoDistanceQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, -70.0, parsed.point().getLon(), 0.0001);
        assertEquals(json, 40.0, parsed.point().getLat(), 0.0001);
        assertEquals(json, 12000.0, parsed.distance(), 0.0001);
    }

    public void testIgnoreUnmapped() throws IOException {
        final GeoDistanceQueryBuilder queryBuilder = new GeoDistanceQueryBuilder("unmapped").point(0.0, 0.0).distance("20m");
        queryBuilder.ignoreUnmapped(true);

        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        Query query = queryBuilder.toQuery(searchExecutionContext);
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final GeoDistanceQueryBuilder failingQueryBuilder = new GeoDistanceQueryBuilder("unmapped").point(0.0, 0.0).distance("20m");
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(searchExecutionContext));
        assertThat(e.getMessage(), containsString("failed to find geo field [unmapped]"));
    }

    public void testParseFailsWithMultipleFields() throws IOException {
        String json = """
            {
              "geo_distance" : {
                "point1" : {
                  "lat" : 30, "lon" : 12
                },
                "point2" : {
                  "lat" : 30, "lon" : 12
                }
              }
            }""";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[geo_distance] query doesn't support multiple fields, found [point1] and [point2]", e.getMessage());
    }
}
