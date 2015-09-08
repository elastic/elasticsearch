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

import com.spatial4j.core.shape.jts.JtsGeometry;
import com.vividsolutions.jts.geom.Coordinate;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.search.geo.GeoPolygonQuery;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.elasticsearch.test.geo.RandomShapeGenerator.ShapeType;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class GeoPolygonQueryBuilderTests extends AbstractQueryTestCase<GeoPolygonQueryBuilder> {

    @Override
    protected GeoPolygonQueryBuilder doCreateTestQueryBuilder() {
        GeoPolygonQueryBuilder builder;
        List<GeoPoint> polygon = randomPolygon(randomIntBetween(4, 50));
        if (randomBoolean()) {
            builder = new GeoPolygonQueryBuilder(GEO_FIELD_NAME, polygon);
        } else {
            builder = new GeoPolygonQueryBuilder(GEO_FIELD_NAME);
            for (GeoPoint point : polygon) {
                int method = randomInt(2);
                switch (method) {
                case 0:
                    builder.addPoint(point);
                    break;
                case 1:
                    builder.addPoint(point.geohash());
                    break;
                case 2:
                    builder.addPoint(point.lat(), point.lon());
                    break;
                }
            }
        }
        builder.coerce(randomBoolean());
        builder.ignoreMalformed(randomBoolean());
        return builder;
    }

    @Override
    protected void doAssertLuceneQuery(GeoPolygonQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(GeoPolygonQuery.class));
        GeoPolygonQuery geoQuery = (GeoPolygonQuery) query;
        assertThat(geoQuery.fieldName(), equalTo(queryBuilder.fieldName()));
        List<GeoPoint> queryBuilderPoints = queryBuilder.points();
        GeoPoint[] queryPoints = geoQuery.points();
        assertThat(queryPoints.length, equalTo(queryBuilderPoints.size()));
        if (queryBuilder.coerce()) {
            for (int i = 0; i < queryBuilderPoints.size(); i++) {
                GeoPoint queryBuilderPoint = queryBuilderPoints.get(i);
                GeoUtils.normalizePoint(queryBuilderPoint, true, true);
                assertThat(queryPoints[i], equalTo(queryBuilderPoint));
            }
        } else {
            for (int i = 0; i < queryBuilderPoints.size(); i++) {
                assertThat(queryPoints[i], equalTo(queryBuilderPoints.get(i)));
            }
        }

    }

    /**
     * Overridden here to ensure the test is only run if at least one type is
     * present in the mappings. Geo queries do not execute if the field is not
     * explicitly mapped
     */
    @Override
    public void testToQuery() throws IOException {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        super.testToQuery();
    }

    public List<GeoPoint> randomPolygon(int numPoints) {
        ShapeBuilder shapeBuilder = null;
        // This is a temporary fix because sometimes the RandomShapeGenerator
        // returns null. This is if there is an error generating the polygon. So
        // in this case keep trying until we successfully generate one
        while (shapeBuilder == null) {
            shapeBuilder = RandomShapeGenerator.createShapeWithin(getRandom(), null, ShapeType.POLYGON);
        }
        JtsGeometry shape = (JtsGeometry) shapeBuilder.build();
        Coordinate[] coordinates = shape.getGeom().getCoordinates();
        ArrayList<GeoPoint> polygonPoints = new ArrayList<>();
        for (Coordinate coord : coordinates) {
            polygonPoints.add(new GeoPoint(coord.y, coord.x));
        }
        return polygonPoints;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullFieldName() {
        new GeoPolygonQueryBuilder(null);
    }

    @Test
    public void testEmptyPolygon() {
        GeoPolygonQueryBuilder builder = new GeoPolygonQueryBuilder(GEO_FIELD_NAME);
        QueryValidationException exception = builder.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), notNullValue());
        assertThat(exception.validationErrors().size(), equalTo(1));
        assertThat(exception.validationErrors().get(0), equalTo("[" + GeoPolygonQueryBuilder.NAME
                + "] no points defined for geo_polygon query"));
    }

    @Test
    public void testInvalidClosedPolygon() {
        GeoPolygonQueryBuilder builder = new GeoPolygonQueryBuilder(GEO_FIELD_NAME);
        builder.addPoint(new GeoPoint(0, 90));
        builder.addPoint(new GeoPoint(90, 90));
        builder.addPoint(new GeoPoint(0, 90));
        QueryValidationException exception = builder.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), notNullValue());
        assertThat(exception.validationErrors().size(), equalTo(1));
        assertThat(exception.validationErrors().get(0), equalTo("[" + GeoPolygonQueryBuilder.NAME
                + "] too few points defined for geo_polygon query"));
    }

    @Test
    public void testInvalidOpenPolygon() {
        GeoPolygonQueryBuilder builder = new GeoPolygonQueryBuilder(GEO_FIELD_NAME);
        builder.addPoint(new GeoPoint(0, 90));
        builder.addPoint(new GeoPoint(90, 90));
        QueryValidationException exception = builder.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), notNullValue());
        assertThat(exception.validationErrors().size(), equalTo(1));
        assertThat(exception.validationErrors().get(0), equalTo("[" + GeoPolygonQueryBuilder.NAME
                + "] too few points defined for geo_polygon query"));
    }

    public void testDeprecatedXContent() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        builder.startObject("geo_polygon");
        builder.startObject(GEO_FIELD_NAME);
        builder.startArray("points");
        builder.value("0,0");
        builder.value("0,90");
        builder.value("90,90");
        builder.value("90,0");
        builder.endArray();
        builder.endObject();
        builder.field("normalize", true); // deprecated
        builder.endObject();
        builder.endObject();
        try {
            parseQuery(builder.string());
            fail("normalize is deprecated");
        } catch (IllegalArgumentException ex) {
            assertEquals("Deprecated field [normalize] used, expected [coerce] instead", ex.getMessage());
        }
    }
}
