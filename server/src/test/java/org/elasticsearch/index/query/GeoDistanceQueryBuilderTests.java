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
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.locationtech.spatial4j.shape.Point;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;

public class GeoDistanceQueryBuilderTests extends AbstractQueryTestCase<GeoDistanceQueryBuilder> {

    @Override
    protected GeoDistanceQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(GEO_POINT_FIELD_NAME, GEO_POINT_ALIAS_FIELD_NAME);
        GeoDistanceQueryBuilder qb = new GeoDistanceQueryBuilder(fieldName);
        String distance = "" + randomDouble();
        if (randomBoolean()) {
            DistanceUnit unit = randomFrom(DistanceUnit.values());
            distance = distance + unit.toString();
        }
        int selector = randomIntBetween(0, 2);
        switch (selector) {
            case 0:
                qb.distance(randomDouble(), randomFrom(DistanceUnit.values()));
                break;
            case 1:
                qb.distance(distance, randomFrom(DistanceUnit.values()));
                break;
            case 2:
                qb.distance(distance);
                break;
        }

        Point p = RandomShapeGenerator.xRandomPoint(random());
        qb.point(new GeoPoint(p.getY(), p.getX()));

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

        e = expectThrows(IllegalArgumentException.class, () -> query.distance(
                randomIntBetween(Integer.MIN_VALUE, 0), DistanceUnit.DEFAULT));
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
    protected void doAssertLuceneQuery(GeoDistanceQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        // TODO: remove the if statement once we always use LatLonPoint
        if (query instanceof IndexOrDocValuesQuery) {
            Query indexQuery = ((IndexOrDocValuesQuery) query).getIndexQuery();

            String expectedFieldName = expectedFieldName(queryBuilder.fieldName());
            assertEquals(LatLonPoint.newDistanceQuery(expectedFieldName,
                    queryBuilder.point().lat(),
                    queryBuilder.point().lon(),
                    queryBuilder.distance()),
                    indexQuery);
            Query dvQuery = ((IndexOrDocValuesQuery) query).getRandomAccessQuery();
            assertEquals(LatLonDocValuesField.newSlowDistanceQuery(expectedFieldName,
                    queryBuilder.point().lat(),
                    queryBuilder.point().lon(),
                    queryBuilder.distance()),
                    dvQuery);
        }
    }

    public void testParsingAndToQuery1() throws IOException {
        String query = "{\n" +
                "    \"geo_distance\":{\n" +
                "        \"distance\":\"12mi\",\n" +
                "        \"" + GEO_POINT_FIELD_NAME + "\":{\n" +
                "            \"lat\":40,\n" +
                "            \"lon\":-70\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        assertGeoDistanceRangeQuery(query, 40, -70, 12, DistanceUnit.MILES);
    }

    public void testParsingAndToQuery2() throws IOException {
        String query = "{\n" +
                "    \"geo_distance\":{\n" +
                "        \"distance\":\"12mi\",\n" +
                "        \"" + GEO_POINT_FIELD_NAME + "\":[-70, 40]\n" +
                "    }\n" +
                "}\n";
        assertGeoDistanceRangeQuery(query, 40, -70, 12, DistanceUnit.MILES);
    }

    public void testParsingAndToQuery3() throws IOException {
        String query = "{\n" +
                "    \"geo_distance\":{\n" +
                "        \"distance\":\"12mi\",\n" +
                "        \"" + GEO_POINT_FIELD_NAME + "\":\"40, -70\"\n" +
                "    }\n" +
                "}\n";
        assertGeoDistanceRangeQuery(query, 40, -70, 12, DistanceUnit.MILES);
    }

    public void testParsingAndToQuery4() throws IOException {
        String query = "{\n" +
                "    \"geo_distance\":{\n" +
                "        \"distance\":\"12mi\",\n" +
                "        \"" + GEO_POINT_FIELD_NAME + "\":\"drn5x1g8cu2y\"\n" +
                "    }\n" +
                "}\n";
        GeoPoint geoPoint = GeoPoint.fromGeohash("drn5x1g8cu2y");
        assertGeoDistanceRangeQuery(query, geoPoint.getLat(), geoPoint.getLon(), 12, DistanceUnit.MILES);
    }

    public void testParsingAndToQuery5() throws IOException {
        String query = "{\n" +
                "    \"geo_distance\":{\n" +
                "        \"distance\":12,\n" +
                "        \"unit\":\"mi\",\n" +
                "        \"" + GEO_POINT_FIELD_NAME + "\":{\n" +
                "            \"lat\":40,\n" +
                "            \"lon\":-70\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        assertGeoDistanceRangeQuery(query, 40, -70, 12, DistanceUnit.MILES);
    }

    public void testParsingAndToQuery6() throws IOException {
        String query = "{\n" +
                "    \"geo_distance\":{\n" +
                "        \"distance\":\"12\",\n" +
                "        \"unit\":\"mi\",\n" +
                "        \"" + GEO_POINT_FIELD_NAME + "\":{\n" +
                "            \"lat\":40,\n" +
                "            \"lon\":-70\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        assertGeoDistanceRangeQuery(query, 40, -70, 12, DistanceUnit.MILES);
    }

    public void testParsingAndToQuery7() throws IOException {
        String query = "{\n" +
                "  \"geo_distance\":{\n" +
                "      \"distance\":\"19.312128\",\n" +
                "      \"" + GEO_POINT_FIELD_NAME + "\":{\n" +
                "          \"lat\":40,\n" +
                "          \"lon\":-70\n" +
                "      }\n" +
                "  }\n" +
                "}\n";
        assertGeoDistanceRangeQuery(query, 40, -70, 19.312128, DistanceUnit.DEFAULT);
    }

    public void testParsingAndToQuery8() throws IOException {
        String query = "{\n" +
                "    \"geo_distance\":{\n" +
                "        \"distance\":19.312128,\n" +
                "        \"" + GEO_POINT_FIELD_NAME + "\":{\n" +
                "            \"lat\":40,\n" +
                "            \"lon\":-70\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        assertGeoDistanceRangeQuery(query, 40, -70, 19.312128, DistanceUnit.DEFAULT);
    }

    public void testParsingAndToQuery9() throws IOException {
        String query = "{\n" +
                "    \"geo_distance\":{\n" +
                "        \"distance\":\"19.312128\",\n" +
                "        \"unit\":\"km\",\n" +
                "        \"" + GEO_POINT_FIELD_NAME + "\":{\n" +
                "            \"lat\":40,\n" +
                "            \"lon\":-70\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        assertGeoDistanceRangeQuery(query, 40, -70, 19.312128, DistanceUnit.KILOMETERS);
    }

    public void testParsingAndToQuery10() throws IOException {
        String query = "{\n" +
                "    \"geo_distance\":{\n" +
                "        \"distance\":19.312128,\n" +
                "        \"unit\":\"km\",\n" +
                "        \"" + GEO_POINT_FIELD_NAME + "\":{\n" +
                "            \"lat\":40,\n" +
                "            \"lon\":-70\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        assertGeoDistanceRangeQuery(query, 40, -70, 19.312128, DistanceUnit.KILOMETERS);
    }

    public void testParsingAndToQuery11() throws IOException {
        String query = "{\n" +
                "    \"geo_distance\":{\n" +
                "        \"distance\":\"19.312128km\",\n" +
                "        \"" + GEO_POINT_FIELD_NAME + "\":{\n" +
                "            \"lat\":40,\n" +
                "            \"lon\":-70\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        assertGeoDistanceRangeQuery(query, 40, -70, 19.312128, DistanceUnit.KILOMETERS);
    }

    public void testParsingAndToQuery12() throws IOException {
        String query = "{\n" +
                "    \"geo_distance\":{\n" +
                "        \"distance\":\"12mi\",\n" +
                "        \"unit\":\"km\",\n" +
                "        \"" + GEO_POINT_FIELD_NAME + "\":{\n" +
                "            \"lat\":40,\n" +
                "            \"lon\":-70\n" +
                "        }\n" +
                "    }\n" +
                "}\n";
        assertGeoDistanceRangeQuery(query, 40, -70, 12, DistanceUnit.MILES);
    }

    private void assertGeoDistanceRangeQuery(String query, double lat, double lon, double distance, DistanceUnit distanceUnit)
            throws IOException {
        Query parsedQuery = parseQuery(query).toQuery(createShardContext());
        // The parsedQuery contains IndexOrDocValuesQuery, which wraps LatLonPointDistanceQuery which in turn has default visibility,
        // so we cannot access its fields directly to check and have to use toString() here instead.
        assertEquals(parsedQuery.toString(),
            "mapped_geo_point:" + lat + "," + lon + " +/- " + distanceUnit.toMeters(distance) + " meters");
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"geo_distance\" : {\n" +
                "    \"pin.location\" : [ -70.0, 40.0 ],\n" +
                "    \"distance\" : 12000.0,\n" +
                "    \"distance_type\" : \"arc\",\n" +
                "    \"validation_method\" : \"STRICT\",\n" +
                "    \"ignore_unmapped\" : false,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";
        GeoDistanceQueryBuilder parsed = (GeoDistanceQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, -70.0, parsed.point().getLon(), 0.0001);
        assertEquals(json, 40.0, parsed.point().getLat(), 0.0001);
        assertEquals(json, 12000.0, parsed.distance(), 0.0001);
    }

    public void testIgnoreUnmapped() throws IOException {
        final GeoDistanceQueryBuilder queryBuilder =
            new GeoDistanceQueryBuilder("unmapped").point(0.0, 0.0).distance("20m");
        queryBuilder.ignoreUnmapped(true);
        QueryShardContext shardContext = createShardContext();
        Query query = queryBuilder.toQuery(shardContext);
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final GeoDistanceQueryBuilder failingQueryBuilder =
            new GeoDistanceQueryBuilder("unmapped").point(0.0, 0.0).distance("20m");
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(shardContext));
        assertThat(e.getMessage(), containsString("failed to find geo_point field [unmapped]"));
    }

    public void testParseFailsWithMultipleFields() throws IOException {
        String json = "{\n" +
                "  \"geo_distance\" : {\n" +
                "    \"point1\" : {\n" +
                "      \"lat\" : 30, \"lon\" : 12\n" +
                "    },\n" +
                "    \"point2\" : {\n" +
                "      \"lat\" : 30, \"lon\" : 12\n" +
                "    }\n" +
                "  }\n" +
                "}";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[geo_distance] query doesn't support multiple fields, found [point1] and [point2]", e.getMessage());
    }
}
