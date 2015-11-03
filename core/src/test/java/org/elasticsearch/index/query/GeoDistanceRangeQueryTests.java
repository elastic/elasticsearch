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

import org.apache.lucene.search.GeoPointDistanceRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.GeoProjectionUtils;
import org.apache.lucene.util.SloppyMath;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.unit.DistanceUnit;

import java.io.IOException;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class GeoDistanceRangeQueryTests extends AbstractQueryTestCase<GeoDistanceRangeQueryBuilder> {

    @Override
    protected GeoDistanceRangeQueryBuilder doCreateTestQueryBuilder() {
        GeoDistanceRangeQueryBuilder builder;
        if (randomBoolean()) {
            builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, randomGeohash(1, 12));
        } else {
            double lat = randomDouble() * 180 - 90;
            double lon = randomDouble() * 360 - 180;
            if (randomBoolean()) {
                builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, new GeoPoint(lat, lon));
            } else {
                builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, lat, lon);
            }
        }
        GeoPoint point = builder.point();
        // TODO remove following hack when LUCENE-6846 lands
        final double distToPole = SloppyMath.haversin(point.lat(), point.lon(), (point.lat()<0) ? -90.0 : 90.0, point.lon());
        final double distCircum = new Double(SloppyMath.haversin(point.lat(), point.lon(), point.lat(), (180.0 + point.lon()) % 360))*1000.0;
        final double maxRadius = Math.min(distToPole, distCircum);

        final int fromValueMeters = randomInt((int)(maxRadius*0.5));
        final int toValueMeters = randomIntBetween(fromValueMeters, (int)maxRadius);
        DistanceUnit fromToUnits = randomFrom(DistanceUnit.values());
        final String fromToUnitsStr = fromToUnits.toString();
        final double fromValue = DistanceUnit.convert(fromValueMeters, DistanceUnit.DEFAULT, fromToUnits);
        final double toValue = DistanceUnit.convert(toValueMeters, DistanceUnit.DEFAULT, fromToUnits);
        if (randomBoolean()) {
            int branch = randomInt(2);
            fromToUnits = DistanceUnit.DEFAULT;
            switch (branch) {
            case 0:
                builder.from(fromValueMeters);
                break;
            case 1:
                builder.to(toValueMeters);
                break;
            case 2:
                builder.from(fromValueMeters);
                builder.to(toValueMeters);
                break;
            }
        } else {
            int branch = randomInt(2);
            switch (branch) {
            case 0:
                builder.from(fromValue + fromToUnitsStr);
                break;
            case 1:
                builder.to(toValue + fromToUnitsStr);
                break;
            case 2:
                builder.from(fromValue + fromToUnitsStr);
                builder.to(toValue + fromToUnitsStr);
                break;
            }
        }
        if (randomBoolean()) {
            builder.includeLower(randomBoolean());
        }
        if (randomBoolean()) {
            builder.includeUpper(randomBoolean());
        }
        if (randomBoolean()) {
            builder.geoDistance(randomFrom(GeoDistance.values()));
        }
        builder.unit(fromToUnits);
        if (randomBoolean()) {
            builder.setValidationMethod(randomFrom(GeoValidationMethod.values()));
        }
        return builder;
    }

    @Override
    protected void doAssertLuceneQuery(GeoDistanceRangeQueryBuilder queryBuilder, Query query, QueryShardContext context)
            throws IOException {
        assertThat(query, instanceOf(GeoPointDistanceRangeQuery.class));
        GeoPointDistanceRangeQuery geoQuery = (GeoPointDistanceRangeQuery) query;
        assertThat(geoQuery.getField(), equalTo(queryBuilder.fieldName()));
        if (queryBuilder.point() != null) {
            GeoPoint expectedPoint = new GeoPoint(queryBuilder.point());
            GeoUtils.normalizePoint(expectedPoint);
            assertThat(geoQuery.getCenterLat(), equalTo(expectedPoint.lat()));
            assertThat(geoQuery.getCenterLon(), equalTo(expectedPoint.lon()));
        }
//        assertThat(geoQuery.getRadiusMeters(), equalTo(queryBuilder.geoDistance()));
        if (queryBuilder.from() != null && queryBuilder.from() instanceof Number) {
            double fromValue = ((Number) queryBuilder.from()).doubleValue();
            if (queryBuilder.unit() != null) {
                fromValue = queryBuilder.unit().toMeters(fromValue);
            }
            if (queryBuilder.geoDistance() != null) {
                fromValue = queryBuilder.geoDistance().normalize(fromValue, DistanceUnit.DEFAULT);
            }
            assertThat(geoQuery.getMinRadiusMeters(), closeTo(fromValue, 1E-5));
        }
        if (queryBuilder.to() != null && queryBuilder.to() instanceof Number) {
            double toValue = ((Number) queryBuilder.to()).doubleValue();
            if (queryBuilder.unit() != null) {
                toValue = queryBuilder.unit().toMeters(toValue);
            }
            if (queryBuilder.geoDistance() != null) {
                toValue = queryBuilder.geoDistance().normalize(toValue, DistanceUnit.DEFAULT);
            }
            assertThat(geoQuery.getMaxRadiusMeters(), closeTo(toValue, 1E-5));
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

    public void testNullFieldName() {
        try {
            if (randomBoolean()) {
                new GeoDistanceRangeQueryBuilder(null, new GeoPoint());
            } else {
                new GeoDistanceRangeQueryBuilder("", new GeoPoint());
            }
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("fieldName must not be null"));
        }
    }

    public void testNoPoint() {
        try {
            if (randomBoolean()) {
                new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, (GeoPoint) null);
            } else {
                new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, (String) null);
            }
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("point must not be null"));
        }
    }

    public void testInvalidFrom() {
        GeoDistanceRangeQueryBuilder builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, new GeoPoint());
        try {
            if (randomBoolean()) {
                builder.from((String) null);
            } else {
                builder.from((Number) null);
            }
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("[from] must not be null"));
        }
    }

    public void testInvalidTo() {
        GeoDistanceRangeQueryBuilder builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, new GeoPoint());
        try {
            if (randomBoolean()) {
                builder.to((String) null);
            } else {
                builder.to((Number) null);
            }
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("[to] must not be null"));
        }
    }

    public void testInvalidGeoDistance() {
        GeoDistanceRangeQueryBuilder builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, new GeoPoint());
        try {
            builder.geoDistance(null);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("geoDistance calculation mode must not be null"));
        }
    }

    public void testInvalidDistanceUnit() {
        GeoDistanceRangeQueryBuilder builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, new GeoPoint());
        try {
            builder.unit(null);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("distance unit must not be null"));
        }
    }
}
