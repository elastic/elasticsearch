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

package org.elasticsearch.index.query;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.locationtech.spatial4j.io.GeohashUtils;
import org.locationtech.spatial4j.shape.Rectangle;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;

public class GeoBoundingBoxQueryBuilderTests extends AbstractQueryTestCase<GeoBoundingBoxQueryBuilder> {
    /** Randomly generate either NaN or one of the two infinity values. */
    private static Double[] brokenDoubles = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};

    @Override
    protected GeoBoundingBoxQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(GEO_POINT_FIELD_NAME, GEO_POINT_ALIAS_FIELD_NAME);
        GeoBoundingBoxQueryBuilder builder = new GeoBoundingBoxQueryBuilder(fieldName);
        Rectangle box = RandomShapeGenerator.xRandomRectangle(random(), RandomShapeGenerator.xRandomPoint(random()));

        if (randomBoolean()) {
            // check the top-left/bottom-right combination of setters
            int path = randomIntBetween(0, 2);
            switch (path) {
            case 0:
                builder.setCorners(
                        new GeoPoint(box.getMaxY(), box.getMinX()),
                        new GeoPoint(box.getMinY(), box.getMaxX()));
                break;
            case 1:
                builder.setCorners(
                        GeohashUtils.encodeLatLon(box.getMaxY(), box.getMinX()),
                        GeohashUtils.encodeLatLon(box.getMinY(), box.getMaxX()));
                break;
            default:
                builder.setCorners(box.getMaxY(), box.getMinX(), box.getMinY(), box.getMaxX());
            }
        } else {
            // check the bottom-left/ top-right combination of setters
            if (randomBoolean()) {
                builder.setCornersOGC(
                        new GeoPoint(box.getMinY(), box.getMinX()),
                        new GeoPoint(box.getMaxY(), box.getMaxX()));
            } else {
                builder.setCornersOGC(
                        GeohashUtils.encodeLatLon(box.getMinY(), box.getMinX()),
                        GeohashUtils.encodeLatLon(box.getMaxY(), box.getMaxX()));
            }
        }

        if (randomBoolean()) {
            builder.setValidationMethod(randomFrom(GeoValidationMethod.values()));
        }

        if (randomBoolean()) {
            builder.ignoreUnmapped(randomBoolean());
        }

        builder.type(randomFrom(GeoExecType.values()));
        return builder;
    }

    public void testValidationNullFieldname() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GeoBoundingBoxQueryBuilder((String) null));
        assertEquals("Field name must not be empty.", e.getMessage());
    }

    public void testValidationNullType() {
        GeoBoundingBoxQueryBuilder qb = new GeoBoundingBoxQueryBuilder("teststring");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> qb.type((GeoExecType) null));
        assertEquals("Type is not allowed to be null.", e.getMessage());
    }

    public void testValidationNullTypeString() {
        GeoBoundingBoxQueryBuilder qb = new GeoBoundingBoxQueryBuilder("teststring");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> qb.type((String) null));
        assertEquals("cannot parse type from null string", e.getMessage());
    }

    public void testExceptionOnMissingTypes() {
        QueryShardContext context = createShardContextWithNoType();
        GeoBoundingBoxQueryBuilder qb = createTestQueryBuilder();
        qb.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> qb.toQuery(context));
        assertEquals("failed to find geo_point field [" + qb.fieldName() + "]", e.getMessage());
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
            assertNull("validation w/ coerce should ignore invalid "
                    + tester.getClass().getName()
                    + " coordinate: "
                    + tester.invalidCoordinate + " ",
                    except);

            tester.invalidateCoordinate(builder.setValidationMethod(GeoValidationMethod.STRICT), false);
            except = builder.checkLatLon();
            assertNotNull("validation w/o coerce should detect invalid coordinate: "
                    + tester.getClass().getName()
                    + " coordinate: "
                    + tester.invalidCoordinate,
                    except);
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
        assertFalse("Someone changed the default for coordinate validation - were the docs changed as well?",
                GeoValidationMethod.DEFAULT_LENIENT_PARSING);
    }

    @Override
    protected void doAssertLuceneQuery(GeoBoundingBoxQueryBuilder queryBuilder, Query query, QueryShardContext context)
        throws IOException {
        MappedFieldType fieldType = context.fieldMapper(queryBuilder.fieldName());
        if (fieldType == null) {
            assertTrue("Found no indexed geo query.", query instanceof MatchNoDocsQuery);
        } else if (query instanceof IndexOrDocValuesQuery) { // TODO: remove the if statement once we always use LatLonPoint
            Query indexQuery = ((IndexOrDocValuesQuery) query).getIndexQuery();
            String expectedFieldName = expectedFieldName(queryBuilder.fieldName());
            assertEquals(LatLonPoint.newBoxQuery(expectedFieldName,
                    queryBuilder.bottomRight().lat(),
                    queryBuilder.topLeft().lat(),
                    queryBuilder.topLeft().lon(),
                    queryBuilder.bottomRight().lon()), indexQuery);
            Query dvQuery = ((IndexOrDocValuesQuery) query).getRandomAccessQuery();
            assertEquals(LatLonDocValuesField.newSlowBoxQuery(expectedFieldName,
                    queryBuilder.bottomRight().lat(),
                    queryBuilder.topLeft().lat(),
                    queryBuilder.topLeft().lon(),
                    queryBuilder.bottomRight().lon()), dvQuery);
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
        String query = "{\n" +
                "    \"geo_bounding_box\":{\n" +
                "        \"" + GEO_POINT_FIELD_NAME+ "\":{\n" +
                "            \"top_left\":[-70, 40],\n" +
                "            \"bottom_right\":[-80, 30]\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        assertGeoBoundingBoxQuery(query);
    }

    public void testParsingAndToQuery2() throws IOException {
        String query = "{\n" +
                "    \"geo_bounding_box\":{\n" +
                "        \"" + GEO_POINT_FIELD_NAME+ "\":{\n" +
                "            \"top_left\":{\n" +
                "                \"lat\":40,\n" +
                "                \"lon\":-70\n" +
                "            },\n" +
                "            \"bottom_right\":{\n" +
                "                \"lat\":30,\n" +
                "                \"lon\":-80\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        assertGeoBoundingBoxQuery(query);
    }

    public void testParsingAndToQuery3() throws IOException {
        String query = "{\n" +
                "    \"geo_bounding_box\":{\n" +
                "        \"" + GEO_POINT_FIELD_NAME+ "\":{\n" +
                "            \"top_left\":\"40, -70\",\n" +
                "            \"bottom_right\":\"30, -80\"\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        assertGeoBoundingBoxQuery(query);
    }

    public void testParsingAndToQuery4() throws IOException {
        String query = "{\n" +
                "    \"geo_bounding_box\":{\n" +
                "        \"" + GEO_POINT_FIELD_NAME+ "\":{\n" +
                "            \"top_left\":\"drn5x1g8cu2y\",\n" +
                "            \"bottom_right\":\"30, -80\"\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        assertGeoBoundingBoxQuery(query);
    }

    public void testParsingAndToQuery5() throws IOException {
        String query = "{\n" +
                "    \"geo_bounding_box\":{\n" +
                "        \"" + GEO_POINT_FIELD_NAME+ "\":{\n" +
                "            \"top_right\":\"40, -80\",\n" +
                "            \"bottom_left\":\"30, -70\"\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        assertGeoBoundingBoxQuery(query);
    }

    public void testParsingAndToQuery6() throws IOException {
        String query = "{\n" +
                "    \"geo_bounding_box\":{\n" +
                "        \"" + GEO_POINT_FIELD_NAME+ "\":{\n" +
                "            \"right\": -80,\n" +
                "            \"top\": 40,\n" +
                "            \"left\": -70,\n" +
                "            \"bottom\": 30\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        assertGeoBoundingBoxQuery(query);
    }

    private void assertGeoBoundingBoxQuery(String query) throws IOException {
        QueryShardContext shardContext = createShardContext();
        // just check if we can parse the query
        parseQuery(query).toQuery(shardContext);
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"geo_bounding_box\" : {\n" +
                "    \"pin.location\" : {\n" +
                "      \"top_left\" : [ -74.1, 40.73 ],\n" +
                "      \"bottom_right\" : [ -71.12, 40.01 ]\n" +
                "    },\n" +
                "    \"validation_method\" : \"STRICT\",\n" +
                "    \"type\" : \"MEMORY\",\n" +
                        "    \"ignore_unmapped\" : false,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";
        GeoBoundingBoxQueryBuilder parsed = (GeoBoundingBoxQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, "pin.location", parsed.fieldName());
        assertEquals(json, -74.1, parsed.topLeft().getLon(), 0.0001);
        assertEquals(json, 40.73, parsed.topLeft().getLat(), 0.0001);
        assertEquals(json, -71.12, parsed.bottomRight().getLon(), 0.0001);
        assertEquals(json, 40.01, parsed.bottomRight().getLat(), 0.0001);
        assertEquals(json, 1.0, parsed.boost(), 0.0001);
        assertEquals(json, GeoExecType.MEMORY, parsed.type());
    }

    public void testFromWKT() throws IOException {
        String wkt =
            "{\n" +
                "  \"geo_bounding_box\" : {\n" +
                "    \"pin.location\" : {\n" +
                "      \"wkt\" : \"BBOX (-74.1, -71.12, 40.73, 40.01)\"\n" +
                "    },\n" +
                "    \"validation_method\" : \"STRICT\",\n" +
                "    \"type\" : \"MEMORY\",\n" +
                "    \"ignore_unmapped\" : false,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        // toXContent generates the query in geojson only; for now we need to test against the expected
        // geojson generated content
        String expectedJson =
            "{\n" +
                "  \"geo_bounding_box\" : {\n" +
                "    \"pin.location\" : {\n" +
                "      \"top_left\" : [ -74.1, 40.73 ],\n" +
                "      \"bottom_right\" : [ -71.12, 40.01 ]\n" +
                "    },\n" +
                "    \"validation_method\" : \"STRICT\",\n" +
                "    \"type\" : \"MEMORY\",\n" +
                "    \"ignore_unmapped\" : false,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

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
        assertEquals(expectedJson, GeoExecType.MEMORY, parsed.type());
    }

    public void testFromGeohash() throws IOException {
        String json =
            "{\n" +
                "  \"geo_bounding_box\" : {\n" +
                "    \"pin.location\" : {\n" +
                "      \"top_left\" : \"dr\",\n" +
                "      \"bottom_right\" : \"dq\"\n" +
                "    },\n" +
                "    \"validation_method\" : \"STRICT\",\n" +
                "    \"type\" : \"MEMORY\",\n" +
                "    \"ignore_unmapped\" : false,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        String expectedJson =
            "{\n" +
                "  \"geo_bounding_box\" : {\n" +
                "    \"pin.location\" : {\n" +
                "      \"top_left\" : [ -78.75, 45.0 ],\n" +
                "      \"bottom_right\" : [ -67.5, 33.75 ]\n" +
                "    },\n" +
                "    \"validation_method\" : \"STRICT\",\n" +
                "    \"type\" : \"MEMORY\",\n" +
                "    \"ignore_unmapped\" : false,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";
        GeoBoundingBoxQueryBuilder parsed = (GeoBoundingBoxQueryBuilder) parseQuery(json);
        checkGeneratedJson(expectedJson, parsed);
        assertEquals(json, "pin.location", parsed.fieldName());
        assertEquals(json, -78.75, parsed.topLeft().getLon(), 0.0001);
        assertEquals(json, 45.0, parsed.topLeft().getLat(), 0.0001);
        assertEquals(json, -67.5, parsed.bottomRight().getLon(), 0.0001);
        assertEquals(json, 33.75, parsed.bottomRight().getLat(), 0.0001);
        assertEquals(json, 1.0, parsed.boost(), 0.0001);
        assertEquals(json, GeoExecType.MEMORY, parsed.type());
    }

    public void testMalformedGeohashes() {
        String jsonGeohashAndWkt =
            "{\n" +
                "  \"geo_bounding_box\" : {\n" +
                "    \"pin.location\" : {\n" +
                "      \"top_left\" : [ -78.75, 45.0 ],\n" +
                "      \"wkt\" : \"BBOX (-74.1, -71.12, 40.73, 40.01)\"\n" +
                "    },\n" +
                "    \"validation_method\" : \"STRICT\",\n" +
                "    \"type\" : \"MEMORY\",\n" +
                "    \"ignore_unmapped\" : false,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        ElasticsearchParseException e1 = expectThrows(ElasticsearchParseException.class, () -> parseQuery(jsonGeohashAndWkt));
        assertThat(e1.getMessage(), containsString("Conflicting definition found using well-known text and explicit corners."));
    }

    public void testHonorsCoercion() throws IOException {
        String query = "{\n" +
            "  \"geo_bounding_box\": {\n" +
            "    \"validation_method\": \"COERCE\",\n" +
            "    \"" + GEO_POINT_FIELD_NAME + "\": {\n" +
            "      \"top_left\": {\n" +
            "        \"lat\": -15.5,\n" +
            "        \"lon\": 176.5\n" +
            "      },\n" +
            "      \"bottom_right\": {\n" +
            "        \"lat\": -19.6,\n" +
            "        \"lon\": 181\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}\n";
        assertGeoBoundingBoxQuery(query);
    }

    @Override
    public void testMustRewrite() throws IOException {
        super.testMustRewrite();
    }

    public void testIgnoreUnmapped() throws IOException {
        final GeoBoundingBoxQueryBuilder queryBuilder = new GeoBoundingBoxQueryBuilder("unmapped").setCorners(1.0, 0.0, 0.0, 1.0);
        queryBuilder.ignoreUnmapped(true);
        QueryShardContext shardContext = createShardContext();
        Query query = queryBuilder.toQuery(shardContext);
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final GeoBoundingBoxQueryBuilder failingQueryBuilder = new GeoBoundingBoxQueryBuilder("unmapped").setCorners(1.0, 0.0, 0.0, 1.0);
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(shardContext));
        assertThat(e.getMessage(), containsString("failed to find geo_point field [unmapped]"));
    }
}
