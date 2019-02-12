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

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.elasticsearch.test.geo.RandomShapeGenerator.ShapeType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

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
    protected void doAssertLuceneQuery(GeoPolygonQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        // todo LatLonPointInPolygon is package private
    }

    private static List<GeoPoint> randomPolygon() {
        ShapeBuilder<?, ?, ?> shapeBuilder = null;
        // This is a temporary fix because sometimes the RandomShapeGenerator
        // returns null. This is if there is an error generating the polygon. So
        // in this case keep trying until we successfully generate one
        while (shapeBuilder == null) {
            shapeBuilder = RandomShapeGenerator.createShapeWithin(random(), null, ShapeType.POLYGON);
        }
        JtsGeometry shape = (JtsGeometry) shapeBuilder.buildS4J();
        Coordinate[] coordinates = shape.getGeom().getCoordinates();
        ArrayList<GeoPoint> polygonPoints = new ArrayList<>();
        for (Coordinate coord : coordinates) {
            polygonPoints.add(new GeoPoint(coord.y, coord.x));
        }
        return polygonPoints;
    }

    public void testNullFieldName() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GeoPolygonQueryBuilder(null, randomPolygon()));
        assertEquals("fieldName must not be null", e.getMessage());
    }

    public void testEmptyPolygon() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new GeoPolygonQueryBuilder(GEO_POINT_FIELD_NAME, Collections.emptyList()));
        assertEquals("polygon must not be null or empty", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new GeoPolygonQueryBuilder(GEO_POINT_FIELD_NAME, null));
        assertEquals("polygon must not be null or empty", e.getMessage());
    }

    public void testInvalidClosedPolygon() {
        List<GeoPoint> points = new ArrayList<>();
        points.add(new GeoPoint(0, 90));
        points.add(new GeoPoint(90, 90));
        points.add(new GeoPoint(0, 90));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new GeoPolygonQueryBuilder(GEO_POINT_FIELD_NAME, points));
        assertEquals("too few points defined for geo_polygon query", e.getMessage());
    }

    public void testInvalidOpenPolygon() {
        List<GeoPoint> points = new ArrayList<>();
        points.add(new GeoPoint(0, 90));
        points.add(new GeoPoint(90, 90));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new GeoPolygonQueryBuilder(GEO_POINT_FIELD_NAME, points));
        assertEquals("too few points defined for geo_polygon query", e.getMessage());
    }

    public void testParsingAndToQueryParsingExceptions() throws IOException {
        String[] brokenFiles = new String[]{
                "/org/elasticsearch/index/query/geo_polygon_exception_1.json",
                "/org/elasticsearch/index/query/geo_polygon_exception_2.json",
                "/org/elasticsearch/index/query/geo_polygon_exception_3.json",
                "/org/elasticsearch/index/query/geo_polygon_exception_4.json",
                "/org/elasticsearch/index/query/geo_polygon_exception_5.json"
        };
        for (String brokenFile : brokenFiles) {
            String query = copyToStringFromClasspath(brokenFile);
            expectThrows(ParsingException.class, () -> parseQuery(query));
        }
    }

    public void testParsingAndToQuery1() throws IOException {
        String query = "{\n" +
                "    \"geo_polygon\":{\n" +
                "        \"" + GEO_POINT_FIELD_NAME + "\":{\n" +
                "            \"points\":[\n" +
                "                [-70, 40],\n" +
                "                [-80, 30],\n" +
                "                [-90, 20]\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        assertGeoPolygonQuery(query);
    }

    public void testParsingAndToQuery2() throws IOException {
        String query = "{\n" +
                "    \"geo_polygon\":{\n" +
                "        \"" + GEO_POINT_FIELD_NAME + "\":{\n" +
                "            \"points\":[\n" +
                "                {\n" +
                "                    \"lat\":40,\n" +
                "                    \"lon\":-70\n" +
                "                },\n" +
                "                {\n" +
                "                    \"lat\":30,\n" +
                "                    \"lon\":-80\n" +
                "                },\n" +
                "                {\n" +
                "                    \"lat\":20,\n" +
                "                    \"lon\":-90\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        assertGeoPolygonQuery(query);
    }

    public void testParsingAndToQuery3() throws IOException {
        String query = "{\n" +
                "    \"geo_polygon\":{\n" +
                "        \"" + GEO_POINT_FIELD_NAME + "\":{\n" +
                "            \"points\":[\n" +
                "                \"40, -70\",\n" +
                "                \"30, -80\",\n" +
                "                \"20, -90\"\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        assertGeoPolygonQuery(query);
    }

    public void testParsingAndToQuery4() throws IOException {
        String query = "{\n" +
                "    \"geo_polygon\":{\n" +
                "        \"" + GEO_POINT_FIELD_NAME + "\":{\n" +
                "            \"points\":[\n" +
                "                \"drn5x1g8cu2y\",\n" +
                "                \"30, -80\",\n" +
                "                \"20, -90\"\n" +
                "            ]\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        assertGeoPolygonQuery(query);
    }

    private void assertGeoPolygonQuery(String query) throws IOException {
        QueryShardContext context = createShardContext();
        parseQuery(query).toQuery(context);
        // TODO LatLonPointInPolygon is package private, need a closeTo check on the query
        // since some points can be computed from the geohash
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"geo_polygon\" : {\n" +
                "    \"person.location\" : {\n" +
                "      \"points\" : [ [ -70.0, 40.0 ], [ -80.0, 30.0 ], [ -90.0, 20.0 ], [ -70.0, 40.0 ] ]\n" +
                "    },\n" +
                "    \"validation_method\" : \"STRICT\",\n" +
                "    \"ignore_unmapped\" : false,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";
        GeoPolygonQueryBuilder parsed = (GeoPolygonQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, 4, parsed.points().size());
    }

    public void testIgnoreUnmapped() throws IOException {
        List<GeoPoint> polygon = randomPolygon();
        final GeoPolygonQueryBuilder queryBuilder = new GeoPolygonQueryBuilder("unmapped", polygon);
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(createShardContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final GeoPolygonQueryBuilder failingQueryBuilder = new GeoPolygonQueryBuilder("unmapped", polygon);
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(createShardContext()));
        assertThat(e.getMessage(), containsString("failed to find geo_point field [unmapped]"));
    }

    public void testPointValidation() throws IOException {
        QueryShardContext context = createShardContext();
        String queryInvalidLat = "{\n" +
            "    \"geo_polygon\":{\n" +
            "        \"" + GEO_POINT_FIELD_NAME + "\":{\n" +
            "            \"points\":[\n" +
            "                [-70, 140],\n" +
            "                [-80, 30],\n" +
            "                [-90, 20]\n" +
            "            ]\n" +
            "        }\n" +
            "    }\n" +
            "}\n";

        QueryShardException e1 = expectThrows(QueryShardException.class, () -> parseQuery(queryInvalidLat).toQuery(context));
        assertThat(e1.getMessage(), containsString("illegal latitude value [140.0] for [geo_polygon]"));

        String queryInvalidLon = "{\n" +
            "    \"geo_polygon\":{\n" +
            "        \"" + GEO_POINT_FIELD_NAME + "\":{\n" +
            "            \"points\":[\n" +
            "                [-70, 40],\n" +
            "                [-80, 30],\n" +
            "                [-190, 20]\n" +
            "            ]\n" +
            "        }\n" +
            "    }\n" +
            "}\n";

        QueryShardException e2 = expectThrows(QueryShardException.class, () -> parseQuery(queryInvalidLon).toQuery(context));
        assertThat(e2.getMessage(), containsString("illegal longitude value [-190.0] for [geo_polygon]"));
    }
}
