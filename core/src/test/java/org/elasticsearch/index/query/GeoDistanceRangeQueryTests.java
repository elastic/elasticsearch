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

import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.search.geo.GeoDistanceRangeQuery;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class GeoDistanceRangeQueryTests extends AbstractQueryTestCase<GeoDistanceRangeQueryBuilder> {

    @Override
    protected GeoDistanceRangeQueryBuilder doCreateTestQueryBuilder() {
        GeoDistanceRangeQueryBuilder builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME);
        if (randomBoolean()) {
            builder.geohash(randomGeohash(1, 12));
        } else {
            double lat = randomDouble() * 180 - 90;
            double lon = randomDouble() * 360 - 180;
            if (randomBoolean()) {
                builder.point(lat, lon);
            } else {
                builder.point(new GeoPoint(lat, lon));
            }
        }
        int fromValue = randomInt(1000000);
        int toValue = randomIntBetween(fromValue, 1000000);
        String fromToUnits = randomFrom(DistanceUnit.values()).toString();
        if (randomBoolean()) {
            int branch = randomInt(2);
            switch (branch) {
            case 0:
                builder.from(fromValue);
                break;
            case 1:
                builder.to(toValue);
                break;
            case 2:
                builder.from(fromValue);
                builder.to(toValue);
                break;
            }
        } else {
            int branch = randomInt(2);
            switch (branch) {
            case 0:
                builder.from(fromValue + fromToUnits);
                break;
            case 1:
                builder.to(toValue + fromToUnits);
                break;
            case 2:
                builder.from(fromValue + fromToUnits);
                builder.to(toValue + fromToUnits);
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
        if (randomBoolean()) {
            builder.unit(randomFrom(DistanceUnit.values()));
        }
        if (randomBoolean()) {
            builder.optimizeBbox(randomFrom("none", "memory", "indexed"));
        }
        if (randomBoolean()) {
            builder.coerce(randomBoolean());
        }
        if (randomBoolean()) {
            builder.ignoreMalformed(randomBoolean());
        }
        return builder;
    }

    @Override
    protected void doAssertLuceneQuery(GeoDistanceRangeQueryBuilder queryBuilder, Query query, QueryShardContext context)
            throws IOException {
        assertThat(query, instanceOf(GeoDistanceRangeQuery.class));
        GeoDistanceRangeQuery geoQuery = (GeoDistanceRangeQuery) query;
        assertThat(geoQuery.fieldName(), equalTo(queryBuilder.fieldName()));
        if (queryBuilder.point() != null) {
            assertThat(geoQuery.lat(), equalTo(queryBuilder.point().lat()));
            assertThat(geoQuery.lon(), equalTo(queryBuilder.point().lon()));
        }
        assertThat(geoQuery.geoDistance(), equalTo(queryBuilder.geoDistance()));
        if (queryBuilder.from() != null && queryBuilder.from() instanceof Number) {
            double fromValue = ((Number) queryBuilder.from()).doubleValue();
            if (queryBuilder.unit() != null) {
                fromValue = queryBuilder.unit().toMeters(fromValue);
            }
            if (queryBuilder.geoDistance() != null) {
                fromValue = queryBuilder.geoDistance().normalize(fromValue, DistanceUnit.DEFAULT);
            }
            assertThat(geoQuery.minInclusiveDistance(), closeTo(fromValue, Math.abs(fromValue) / 1000));
        }
        if (queryBuilder.to() != null && queryBuilder.to() instanceof Number) {
            double toValue = ((Number) queryBuilder.to()).doubleValue();
            if (queryBuilder.unit() != null) {
                toValue = queryBuilder.unit().toMeters(toValue);
            }
            if (queryBuilder.geoDistance() != null) {
                toValue = queryBuilder.geoDistance().normalize(toValue, DistanceUnit.DEFAULT);
            }
            assertThat(geoQuery.maxInclusiveDistance(), closeTo(toValue, Math.abs(toValue) / 1000));
        }
    }

    /**
     * Overridden here to ensure the test is only run if at least one type is
     * present in the mappings. Geo queries do not execute if the field is not
     * explicitly mapped
     */
    @Override
    @Test
    public void testToQuery() throws IOException {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        super.testToQuery();
    }

    @Test
    public void testNullFieldName() {
        GeoDistanceRangeQueryBuilder builder = new GeoDistanceRangeQueryBuilder(null);
        builder.geohash(randomGeohash(1, 20));
        builder.from(10);
        QueryValidationException exception = builder.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), notNullValue());
        assertThat(exception.validationErrors().size(), equalTo(1));
        assertThat(exception.validationErrors().get(0), equalTo("[" + GeoDistanceRangeQueryBuilder.NAME + "] fieldName must not be null"));
    }

    @Test
    public void testNoPoint() {
        GeoDistanceRangeQueryBuilder builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME);
        builder.from(10);
        QueryValidationException exception = builder.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), notNullValue());
        assertThat(exception.validationErrors().size(), equalTo(1));
        assertThat(exception.validationErrors().get(0), equalTo("[" + GeoDistanceRangeQueryBuilder.NAME + "] point must not be null"));
    }

    @Test
    public void testNoFromOrTo() {
        GeoDistanceRangeQueryBuilder builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME);
        String geohash = randomGeohash(1, 20);
        builder.geohash(geohash);
        QueryValidationException exception = builder.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), notNullValue());
        assertThat(exception.validationErrors().size(), equalTo(1));
        assertThat(exception.validationErrors().get(0), equalTo("[" + GeoDistanceRangeQueryBuilder.NAME
                + "] Must define at least one parameter from [from, to]"));
    }

    @Test
    public void testInvalidFrom() {
        GeoDistanceRangeQueryBuilder builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME);
        String geohash = randomGeohash(1, 20);
        builder.geohash(geohash);
        builder.from(new DateTime());
        QueryValidationException exception = builder.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), notNullValue());
        assertThat(exception.validationErrors().size(), equalTo(1));
        assertThat(exception.validationErrors().get(0), equalTo("[" + GeoDistanceRangeQueryBuilder.NAME
                + "] from must either be a number or a string. Found [" + DateTime.class.getName() + "]"));
    }

    @Test
    public void testInvalidTo() {
        GeoDistanceRangeQueryBuilder builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME);
        String geohash = randomGeohash(1, 20);
        builder.geohash(geohash);
        builder.to(new DateTime());
        QueryValidationException exception = builder.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), notNullValue());
        assertThat(exception.validationErrors().size(), equalTo(1));
        assertThat(exception.validationErrors().get(0), equalTo("[" + GeoDistanceRangeQueryBuilder.NAME
                + "] to must either be a number or a string. Found [" + DateTime.class.getName() + "]"));
    }

    @Test
    public void testInvalidOptimizeBBox() {
        GeoDistanceRangeQueryBuilder builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME);
        String geohash = randomGeohash(1, 20);
        builder.geohash(geohash);
        builder.from(10);
        builder.optimizeBbox("foo");
        QueryValidationException exception = builder.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), notNullValue());
        assertThat(exception.validationErrors().size(), equalTo(1));
        assertThat(exception.validationErrors().get(0), equalTo("[" + GeoDistanceRangeQueryBuilder.NAME
                + "] optimizeBbox must be one of [none, memory, indexed]"));
    }

    @Test
    public void testMultipleValidationErrors() {
        GeoDistanceRangeQueryBuilder builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME);
        double lat = randomDouble() * 360 - 180;
        double lon = randomDouble() * 360 - 180;
        builder.point(lat, lon);
        builder.from(new DateTime());
        builder.to(new DateTime());
        builder.optimizeBbox("foo");
        QueryValidationException exception = builder.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), notNullValue());
        assertThat(exception.validationErrors().size(), equalTo(3));
    }
}
