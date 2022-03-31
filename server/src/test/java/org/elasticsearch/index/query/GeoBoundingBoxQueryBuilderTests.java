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
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;

public class GeoBoundingBoxQueryBuilderTests extends AbstractQueryTestCase<GeoBoundingBoxQueryBuilder> {
    /** Randomly generate either NaN or one of the two infinity values. */
    private static Double[] brokenDoubles = { Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY };

    @Override
    protected GeoBoundingBoxQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(GEO_POINT_FIELD_NAME, GEO_POINT_ALIAS_FIELD_NAME, GEO_SHAPE_FIELD_NAME);
        GeoBoundingBoxQueryBuilder builder = new GeoBoundingBoxQueryBuilder(fieldName);
        // make sure that minX != maxX and minY != maxY after geohash encoding
        Rectangle box = randomValueOtherThanMany(
            (r) -> Math.abs(r.getMaxX() - r.getMinX()) < 0.1 || Math.abs(r.getMaxY() - r.getMinY()) < 0.1,
            GeometryTestUtils::randomRectangle
        );

        if (randomBoolean()) {
            // check the top-left/bottom-right combination of setters
            int path = randomIntBetween(0, 2);
            switch (path) {
                case 0 -> builder.setCorners(new GeoPoint(box.getMaxY(), box.getMinX()), new GeoPoint(box.getMinY(), box.getMaxX()));
                case 1 -> builder.setCorners(
                    Geohash.stringEncode(box.getMinX(), box.getMaxY()),
                    Geohash.stringEncode(box.getMaxX(), box.getMinY())
                );
                default -> builder.setCorners(box.getMaxY(), box.getMinX(), box.getMinY(), box.getMaxX());
            }
        } else {
            // check the bottom-left/ top-right combination of setters
            if (randomBoolean()) {
                builder.setCornersOGC(new GeoPoint(box.getMinY(), box.getMinX()), new GeoPoint(box.getMaxY(), box.getMaxX()));
            } else {
                builder.setCornersOGC(
                    Geohash.stringEncode(box.getMinX(), box.getMinY()),
                    Geohash.stringEncode(box.getMaxX(), box.getMaxY())
                );
            }
        }

        if (randomBoolean()) {
            builder.setValidationMethod(randomFrom(GeoValidationMethod.values()));
        }

        if (randomBoolean()) {
            builder.ignoreUnmapped(randomBoolean());
        }

        return builder;
    }

    public void testValidationNullFieldname() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GeoBoundingBoxQueryBuilder((String) null));
        assertEquals("Field name must not be empty.", e.getMessage());
    }

    public void testExceptionOnMissingTypes() {
        SearchExecutionContext context = createShardContextWithNoType();
        GeoBoundingBoxQueryBuilder qb = createTestQueryBuilder();
        qb.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> qb.toQuery(context));
        assertEquals("failed to find geo field [" + qb.fieldName() + "]", e.getMessage());
    }

    public void testBrokenCoordinateCannotBeSet() {
        PointTester[] testers = { new TopTester(), new LeftTester(), new BottomTester(), new RightTester() };
        GeoBoundingBoxQueryBuilder builder = createTestQueryBuilder();
        builder.setValidationMethod(GeoValidationMethod.STRICT);

        for (PointTester tester : testers) {
            expectThrows(IllegalArgumentException.class, () -> tester.invalidateCoordinate(builder, true));
        }
    }

    public void testBrokenCoordinateCanBeSetWithIgnoreMalformed() {
        PointTester[] testers = { new TopTester(), new LeftTester(), new BottomTester(), new RightTester() };

        GeoBoundingBoxQueryBuilder builder = createTestQueryBuilder();
        builder.setValidationMethod(GeoValidationMethod.IGNORE_MALFORMED);

        for (PointTester tester : testers) {
            tester.invalidateCoordinate(builder, true);
        }
    }

    public void testValidation() {
        PointTester[] testers = { new TopTester(), new LeftTester(), new BottomTester(), new RightTester() };

        for (PointTester tester : testers) {
            QueryValidationException except = null;

            GeoBoundingBoxQueryBuilder builder = createTestQueryBuilder();
            tester.invalidateCoordinate(builder.setValidationMethod(GeoValidationMethod.COERCE), false);
            except = builder.checkLatLon();
            assertNull(
                "validation w/ coerce should ignore invalid "
                    + tester.getClass().getName()
                    + " coordinate: "
                    + tester.invalidCoordinate
                    + " ",
                except
            );

            tester.invalidateCoordinate(builder.setValidationMethod(GeoValidationMethod.STRICT), false);
            except = builder.checkLatLon();
            assertNotNull(
                "validation w/o coerce should detect invalid coordinate: "
                    + tester.getClass().getName()
                    + " coordinate: "
                    + tester.invalidCoordinate,
                except
            );
        }
    }

    public void testTopBottomCannotBeFlipped() {
        GeoBoundingBoxQueryBuilder builder = createTestQueryBuilder();
        double top = builder.topLeft().getLat();
        double left = builder.topLeft().getLon();
        double bottom = builder.bottomRight().getLat();
        double right = builder.bottomRight().getLon();

        assumeTrue("top should not be equal to bottom for flip check", top != bottom);
        logger.info("top: {} bottom: {}", top, bottom);
        builder.setValidationMethod(GeoValidationMethod.STRICT);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.setCorners(bottom, left, top, right));
        assertThat(e.getMessage(), containsString("top is below bottom corner:"));
    }

    public void testTopBottomCanBeFlippedOnIgnoreMalformed() {
        GeoBoundingBoxQueryBuilder builder = createTestQueryBuilder();
        double top = builder.topLeft().getLat();
        double left = builder.topLeft().getLon();
        double bottom = builder.bottomRight().getLat();
        double right = builder.bottomRight().getLon();

        assumeTrue("top should not be equal to bottom for flip check", top != bottom);
        builder.setValidationMethod(GeoValidationMethod.IGNORE_MALFORMED).setCorners(bottom, left, top, right);
    }

    public void testLeftRightCanBeFlipped() {
        GeoBoundingBoxQueryBuilder builder = createTestQueryBuilder();
        double top = builder.topLeft().getLat();
        double left = builder.topLeft().getLon();
        double bottom = builder.bottomRight().getLat();
        double right = builder.bottomRight().getLon();

        builder.setValidationMethod(GeoValidationMethod.IGNORE_MALFORMED).setCorners(top, right, bottom, left);
        builder.setValidationMethod(GeoValidationMethod.STRICT).setCorners(top, right, bottom, left);
    }

    public void testStrictnessDefault() {
        assertFalse(
            "Someone changed the default for coordinate validation - were the docs changed as well?",
            GeoValidationMethod.DEFAULT_LENIENT_PARSING
        );
    }

    @Override
    protected void doAssertLuceneQuery(GeoBoundingBoxQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        final MappedFieldType fieldType = context.getFieldType(queryBuilder.fieldName());
        if (fieldType == null) {
            assertTrue("Found no indexed geo query.", query instanceof MatchNoDocsQuery);
        } else if (fieldType instanceof GeoPointFieldMapper.GeoPointFieldType) {
            assertEquals(IndexOrDocValuesQuery.class, query.getClass());
            Query indexQuery = ((IndexOrDocValuesQuery) query).getIndexQuery();
            String expectedFieldName = expectedFieldName(queryBuilder.fieldName());
            assertEquals(
                LatLonPoint.newBoxQuery(
                    expectedFieldName,
                    queryBuilder.bottomRight().lat(),
                    queryBuilder.topLeft().lat(),
                    queryBuilder.topLeft().lon(),
                    queryBuilder.bottomRight().lon()
                ),
                indexQuery
            );
            Query dvQuery = ((IndexOrDocValuesQuery) query).getRandomAccessQuery();
            assertEquals(
                LatLonDocValuesField.newSlowBoxQuery(
                    expectedFieldName,
                    queryBuilder.bottomRight().lat(),
                    queryBuilder.topLeft().lat(),
                    queryBuilder.topLeft().lon(),
                    queryBuilder.bottomRight().lon()
                ),
                dvQuery
            );
        } else {
            assertEquals(GeoShapeFieldMapper.GeoShapeFieldType.class, fieldType.getClass());
        }
    }

    public abstract class PointTester {
        private double brokenCoordinate = randomFrom(brokenDoubles);
        private double invalidCoordinate;

        public PointTester(double invalidCoodinate) {
            this.invalidCoordinate = invalidCoodinate;
        }

        public void invalidateCoordinate(GeoBoundingBoxQueryBuilder qb, boolean useBrokenDouble) {
            if (useBrokenDouble) {
                fillIn(brokenCoordinate, qb);
            } else {
                fillIn(invalidCoordinate, qb);
            }
        }

        protected abstract void fillIn(double fillIn, GeoBoundingBoxQueryBuilder qb);
    }

    public class TopTester extends PointTester {
        public TopTester() {
            super(randomDoubleBetween(GeoUtils.MAX_LAT, Double.MAX_VALUE, false));
        }

        @Override
        public void fillIn(double coordinate, GeoBoundingBoxQueryBuilder qb) {
            qb.setCorners(coordinate, qb.topLeft().getLon(), qb.bottomRight().getLat(), qb.bottomRight().getLon());
        }
    }

    public class LeftTester extends PointTester {
        public LeftTester() {
            super(randomDoubleBetween(-Double.MAX_VALUE, GeoUtils.MIN_LON, true));
        }

        @Override
        public void fillIn(double coordinate, GeoBoundingBoxQueryBuilder qb) {
            qb.setCorners(qb.topLeft().getLat(), coordinate, qb.bottomRight().getLat(), qb.bottomRight().getLon());
        }
    }

    public class BottomTester extends PointTester {
        public BottomTester() {
            super(randomDoubleBetween(-Double.MAX_VALUE, GeoUtils.MIN_LAT, false));
        }

        @Override
        public void fillIn(double coordinate, GeoBoundingBoxQueryBuilder qb) {
            qb.setCorners(qb.topLeft().getLat(), qb.topLeft().getLon(), coordinate, qb.bottomRight().getLon());
        }
    }

    public class RightTester extends PointTester {
        public RightTester() {
            super(randomDoubleBetween(GeoUtils.MAX_LON, Double.MAX_VALUE, true));
        }

        @Override
        public void fillIn(double coordinate, GeoBoundingBoxQueryBuilder qb) {
            qb.setCorners(qb.topLeft().getLat(), qb.topLeft().getLon(), qb.bottomRight().getLat(), coordinate);
        }
    }

    public void testParsingAndToQuery1() throws IOException {
        String query = """
            {
                "geo_bounding_box":{
                    "%s":{
                        "top_left":[-70, 40],
                        "bottom_right":[-80, 30]
                    }
                }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        assertGeoBoundingBoxQuery(query);
    }

    public void testParsingAndToQuery2() throws IOException {
        String query = """
            {
                "geo_bounding_box":{
                    "%s":{
                        "top_left":{
                            "lat":40,
                            "lon":-70
                        },
                        "bottom_right":{
                            "lat":30,
                            "lon":-80
                        }
                    }
                }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        assertGeoBoundingBoxQuery(query);
    }

    public void testParsingAndToQuery3() throws IOException {
        String query = """
            {
                "geo_bounding_box":{
                    "%s":{
                        "top_left":"40, -70",
                        "bottom_right":"30, -80"
                    }
                }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        assertGeoBoundingBoxQuery(query);
    }

    public void testParsingAndToQuery4() throws IOException {
        String query = """
            {
                "geo_bounding_box":{
                    "%s":{
                        "top_left":"drn5x1g8cu2y",
                        "bottom_right":"30, -80"
                    }
                }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        assertGeoBoundingBoxQuery(query);
    }

    public void testParsingAndToQuery5() throws IOException {
        String query = """
            {
                "geo_bounding_box":{
                    "%s":{
                        "top_right":"40, -80",
                        "bottom_left":"30, -70"
                    }
                }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        assertGeoBoundingBoxQuery(query);
    }

    public void testParsingAndToQuery6() throws IOException {
        String query = """
            {
                "geo_bounding_box":{
                    "%s":{
                        "right": -80,
                        "top": 40,
                        "left": -70,
                        "bottom": 30
                    }
                }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        assertGeoBoundingBoxQuery(query);
    }

    private void assertGeoBoundingBoxQuery(String query) throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        // just check if we can parse the query
        parseQuery(query).toQuery(searchExecutionContext);
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "geo_bounding_box" : {
                "pin.location" : {
                  "top_left" : [ -74.1, 40.73 ],
                  "bottom_right" : [ -71.12, 40.01 ]
                },
                "validation_method" : "STRICT",
                "ignore_unmapped" : false,
                "boost" : 1.0
              }
            }""";
        GeoBoundingBoxQueryBuilder parsed = (GeoBoundingBoxQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, "pin.location", parsed.fieldName());
        assertEquals(json, -74.1, parsed.topLeft().getLon(), 0.0001);
        assertEquals(json, 40.73, parsed.topLeft().getLat(), 0.0001);
        assertEquals(json, -71.12, parsed.bottomRight().getLon(), 0.0001);
        assertEquals(json, 40.01, parsed.bottomRight().getLat(), 0.0001);
        assertEquals(json, 1.0, parsed.boost(), 0.0001);
    }

    public void testFromWKT() throws IOException {
        String wkt = """
            {
              "geo_bounding_box" : {
                "pin.location" : {
                  "wkt" : "BBOX (-74.1, -71.12, 40.73, 40.01)"
                },
                "validation_method" : "STRICT",
                "ignore_unmapped" : false,
                "boost" : 1.0
              }
            }""";

        // toXContent generates the query in geojson only; for now we need to test against the expected
        // geojson generated content
        String expectedJson = """
            {
              "geo_bounding_box" : {
                "pin.location" : {
                  "top_left" : [ -74.1, 40.73 ],
                  "bottom_right" : [ -71.12, 40.01 ]
                },
                "validation_method" : "STRICT",
                "ignore_unmapped" : false,
                "boost" : 1.0
              }
            }""";

        // parse with wkt
        GeoBoundingBoxQueryBuilder parsed = (GeoBoundingBoxQueryBuilder) parseQuery(wkt);
        // check the builder's generated geojson content against the expected json output
        checkGeneratedJson(expectedJson, parsed);
        double delta = 0d;
        assertEquals(expectedJson, "pin.location", parsed.fieldName());
        assertEquals(expectedJson, -74.1, parsed.topLeft().getLon(), delta);
        assertEquals(expectedJson, 40.73, parsed.topLeft().getLat(), delta);
        assertEquals(expectedJson, -71.12, parsed.bottomRight().getLon(), delta);
        assertEquals(expectedJson, 40.01, parsed.bottomRight().getLat(), delta);
        assertEquals(expectedJson, 1.0, parsed.boost(), delta);
    }

    public void testFromGeohash() throws IOException {
        String json = """
            {
              "geo_bounding_box" : {
                "pin.location" : {
                  "top_left" : "dr",
                  "bottom_right" : "dq"
                },
                "validation_method" : "STRICT",
                "ignore_unmapped" : false,
                "boost" : 1.0
              }
            }""";

        String expectedJson = """
            {
              "geo_bounding_box" : {
                "pin.location" : {
                  "top_left" : [ -78.75, 45.0 ],
                  "bottom_right" : [ -67.5, 33.75 ]
                },
                "validation_method" : "STRICT",
                "ignore_unmapped" : false,
                "boost" : 1.0
              }
            }""";
        GeoBoundingBoxQueryBuilder parsed = (GeoBoundingBoxQueryBuilder) parseQuery(json);
        checkGeneratedJson(expectedJson, parsed);
        assertEquals(json, "pin.location", parsed.fieldName());
        assertEquals(json, -78.75, parsed.topLeft().getLon(), 0.0001);
        assertEquals(json, 45.0, parsed.topLeft().getLat(), 0.0001);
        assertEquals(json, -67.5, parsed.bottomRight().getLon(), 0.0001);
        assertEquals(json, 33.75, parsed.bottomRight().getLat(), 0.0001);
        assertEquals(json, 1.0, parsed.boost(), 0.0001);
    }

    public void testMalformedGeohashes() {
        String jsonGeohashAndWkt = """
            {
              "geo_bounding_box" : {
                "pin.location" : {
                  "top_left" : [ -78.75, 45.0 ],
                  "wkt" : "BBOX (-74.1, -71.12, 40.73, 40.01)"
                },
                "validation_method" : "STRICT",
                "ignore_unmapped" : false,
                "boost" : 1.0
              }
            }""";

        ElasticsearchParseException e1 = expectThrows(ElasticsearchParseException.class, () -> parseQuery(jsonGeohashAndWkt));
        assertThat(e1.getMessage(), containsString("Conflicting definition found using well-known text and explicit corners."));
    }

    public void testHonorsCoercion() throws IOException {
        String query = """
            {
              "geo_bounding_box": {
                "validation_method": "COERCE",
                "%s": {
                  "top_left": {
                    "lat": -15.5,
                    "lon": 176.5
                  },
                  "bottom_right": {
                    "lat": -19.6,
                    "lon": 181
                  }
                }
              }
            }
            """.formatted(GEO_POINT_FIELD_NAME);
        assertGeoBoundingBoxQuery(query);
    }

    @Override
    public void testMustRewrite() throws IOException {
        super.testMustRewrite();
    }

    public void testIgnoreUnmapped() throws IOException {
        final GeoBoundingBoxQueryBuilder queryBuilder = new GeoBoundingBoxQueryBuilder("unmapped").setCorners(1.0, 0.0, 0.0, 1.0);
        queryBuilder.ignoreUnmapped(true);
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        Query query = queryBuilder.toQuery(searchExecutionContext);
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final GeoBoundingBoxQueryBuilder failingQueryBuilder = new GeoBoundingBoxQueryBuilder("unmapped").setCorners(1.0, 0.0, 0.0, 1.0);
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(searchExecutionContext));
        assertThat(e.getMessage(), containsString("failed to find geo field [unmapped]"));
    }
}
