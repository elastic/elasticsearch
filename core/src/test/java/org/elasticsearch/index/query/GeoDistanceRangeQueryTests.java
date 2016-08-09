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
import org.apache.lucene.spatial.geopoint.search.XGeoPointDistanceRangeQuery;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.search.geo.GeoDistanceRangeQuery;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.geo.RandomGeoGenerator;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class GeoDistanceRangeQueryTests extends AbstractQueryTestCase<GeoDistanceRangeQueryBuilder> {

    @Override
    protected GeoDistanceRangeQueryBuilder doCreateTestQueryBuilder() {
        Version version = createShardContext().indexVersionCreated();
        GeoDistanceRangeQueryBuilder builder;
        GeoPoint randomPoint = RandomGeoGenerator.randomPointIn(random(), -180.0, -89.9, 180.0, 89.9);
        if (randomBoolean()) {
            builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, randomPoint.geohash());
        } else {
            if (randomBoolean()) {
                builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, randomPoint);
            } else {
                builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, randomPoint.lat(), randomPoint.lon());
            }
        }
        GeoPoint point = builder.point();
        final double maxRadius = GeoUtils.maxRadialDistanceMeters(point.lat(), point.lon());
        final int fromValueMeters = randomInt((int)(maxRadius*0.5));
        final int toValueMeters = randomIntBetween(fromValueMeters + 1, (int)maxRadius);
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
        if (randomBoolean() && version.before(Version.V_2_2_0)) {
            builder.optimizeBbox(randomFrom("none", "memory", "indexed"));
        }
        builder.unit(fromToUnits);
        if (randomBoolean()) {
            builder.setValidationMethod(randomFrom(GeoValidationMethod.values()));
        }

        if (randomBoolean()) {
            builder.ignoreUnmapped(randomBoolean());
        }
        return builder;
    }

    @Override
    protected void doAssertLuceneQuery(GeoDistanceRangeQueryBuilder queryBuilder, Query query, QueryShardContext context)
            throws IOException {
        Version version = context.indexVersionCreated();
        if (version.before(Version.V_2_2_0)) {
            assertLegacyQuery(queryBuilder, query);
        } else {
            assertGeoPointQuery(queryBuilder, query);
        }
    }

    private void assertLegacyQuery(GeoDistanceRangeQueryBuilder queryBuilder, Query query) throws IOException {
        assertThat(query, instanceOf(GeoDistanceRangeQuery.class));
        GeoDistanceRangeQuery geoQuery = (GeoDistanceRangeQuery) query;
        assertThat(geoQuery.fieldName(), equalTo(queryBuilder.fieldName()));
        if (queryBuilder.point() != null) {
            GeoPoint expectedPoint = new GeoPoint(queryBuilder.point());
            if (GeoValidationMethod.isCoerce(queryBuilder.getValidationMethod())) {
                GeoUtils.normalizePoint(expectedPoint, true, true);
            }
            assertThat(geoQuery.lat(), equalTo(expectedPoint.lat()));
            assertThat(geoQuery.lon(), equalTo(expectedPoint.lon()));
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
            double fromSlop = Math.abs(fromValue) / 1000;
            if (queryBuilder.includeLower() == false) {
                fromSlop = NumericUtils.sortableLongToDouble((NumericUtils.doubleToSortableLong(Math.abs(fromValue)) + 1L)) / 1000.0;
            }
            assertThat(geoQuery.minInclusiveDistance(), closeTo(fromValue, fromSlop));
        }
        if (queryBuilder.to() != null && queryBuilder.to() instanceof Number) {
            double toValue = ((Number) queryBuilder.to()).doubleValue();
            if (queryBuilder.unit() != null) {
                toValue = queryBuilder.unit().toMeters(toValue);
            }
            if (queryBuilder.geoDistance() != null) {
                toValue = queryBuilder.geoDistance().normalize(toValue, DistanceUnit.DEFAULT);
            }
            double toSlop = Math.abs(toValue) / 1000;
            if (queryBuilder.includeUpper() == false) {
                toSlop = NumericUtils.sortableLongToDouble((NumericUtils.doubleToSortableLong(Math.abs(toValue)) - 1L)) / 1000.0;
            }
            assertThat(geoQuery.maxInclusiveDistance(), closeTo(toValue, toSlop));
        }
    }

    private void assertGeoPointQuery(GeoDistanceRangeQueryBuilder queryBuilder, Query query) throws IOException {
        assertThat(query, instanceOf(XGeoPointDistanceRangeQuery.class));
        XGeoPointDistanceRangeQuery geoQuery = (XGeoPointDistanceRangeQuery) query;
        assertThat(geoQuery.getField(), equalTo(queryBuilder.fieldName()));
        if (queryBuilder.point() != null) {
            GeoPoint expectedPoint = new GeoPoint(queryBuilder.point());
            GeoUtils.normalizePoint(expectedPoint);
            assertThat(geoQuery.getCenterLat(), equalTo(expectedPoint.lat()));
            assertThat(geoQuery.getCenterLon(), equalTo(expectedPoint.lon()));
        }
        if (queryBuilder.from() != null && queryBuilder.from() instanceof Number) {
            double fromValue = ((Number) queryBuilder.from()).doubleValue();
            if (queryBuilder.unit() != null) {
                fromValue = queryBuilder.unit().toMeters(fromValue);
            }
            assertThat(geoQuery.getMinRadiusMeters(), closeTo(fromValue, 1E-5));
        }
        if (queryBuilder.to() != null && queryBuilder.to() instanceof Number) {
            double toValue = ((Number) queryBuilder.to()).doubleValue();
            if (queryBuilder.unit() != null) {
                toValue = queryBuilder.unit().toMeters(toValue);
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
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new GeoDistanceRangeQueryBuilder(null, new GeoPoint()));
        assertEquals("fieldName must not be null", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> new GeoDistanceRangeQueryBuilder("", new GeoPoint()));
        assertEquals("fieldName must not be null", e.getMessage());
    }

    public void testNoPoint() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, (GeoPoint) null));
        assertEquals("point must not be null", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, (String) null));
        assertEquals("point must not be null", e.getMessage());
    }

    public void testInvalidFrom() {
        GeoDistanceRangeQueryBuilder builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, new GeoPoint());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.from((String) null));
        assertEquals("[from] must not be null", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> builder.from((Number) null));
        assertEquals("[from] must not be null", e.getMessage());
    }

    public void testInvalidTo() {
        GeoDistanceRangeQueryBuilder builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, new GeoPoint());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.to((String) null));
        assertEquals("[to] must not be null", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> builder.to((Number) null));
        assertEquals("[to] must not be null", e.getMessage());
    }

    public void testInvalidOptimizeBBox() {
        GeoDistanceRangeQueryBuilder builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, new GeoPoint());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.optimizeBbox(null));
        assertEquals("optimizeBbox must not be null", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> builder.optimizeBbox("foo"));
        assertEquals("optimizeBbox must be one of [none, memory, indexed]", e.getMessage());
    }

    public void testInvalidGeoDistance() {
        GeoDistanceRangeQueryBuilder builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, new GeoPoint());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.geoDistance(null));
        assertEquals("geoDistance calculation mode must not be null", e.getMessage());
    }

    public void testInvalidDistanceUnit() {
        GeoDistanceRangeQueryBuilder builder = new GeoDistanceRangeQueryBuilder(GEO_POINT_FIELD_NAME, new GeoPoint());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.unit(null));
        assertEquals("distance unit must not be null", e.getMessage());
    }

    public void testNestedRangeQuery() throws IOException {
        // create a nested geo_point type with a subfield named "geohash" (explicit testing for ISSUE #15179)
        MapperService mapperService = createShardContext().getMapperService();
        String nestedMapping =
            "{\"nested_doc\" : {\"properties\" : {" +
            "\"locations\": {\"properties\": {" +
            "\"geohash\": {\"type\": \"geo_point\"}}," +
            "\"type\": \"nested\"}" +
            "}}}";
        mapperService.merge("nested_doc", new CompressedXContent(nestedMapping), MapperService.MergeReason.MAPPING_UPDATE, false);

        // create a range query on the nested locations.geohash sub-field
        String queryJson =
            "{\n" +
            "  \"nested\": {\n" +
            "    \"path\": \"locations\",\n" +
            "    \"query\": {\n" +
            "      \"geo_distance_range\": {\n" +
            "        \"from\": \"0.0km\",\n" +
            "        \"to\" : \"200.0km\",\n" +
            "        \"locations.geohash\": \"s7ws01wyd7ws\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}\n";
        NestedQueryBuilder builder = (NestedQueryBuilder) parseQuery(queryJson);
        QueryShardContext context = createShardContext();
        builder.toQuery(context);
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"geo_distance_range\" : {\n" +
                "    \"pin.location\" : [ -70.0, 40.0 ],\n" +
                "    \"from\" : \"200km\",\n" +
                "    \"to\" : \"400km\",\n" +
                "    \"include_lower\" : true,\n" +
                "    \"include_upper\" : true,\n" +
                "    \"unit\" : \"m\",\n" +
                "    \"distance_type\" : \"sloppy_arc\",\n" +
                "    \"optimize_bbox\" : \"memory\",\n" +
                "    \"validation_method\" : \"STRICT\",\n" +
                "    \"ignore_unmapped\" : false,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";
        GeoDistanceRangeQueryBuilder parsed = (GeoDistanceRangeQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, -70.0, parsed.point().lon(), 0.0001);
    }

    public void testFromJsonCoerceFails() throws IOException {
        String json =
                "{\n" +
                "  \"geo_distance_range\" : {\n" +
                "    \"pin.location\" : [ -70.0, 40.0 ],\n" +
                "    \"from\" : \"200km\",\n" +
                "    \"to\" : \"400km\",\n" +
                "    \"include_lower\" : true,\n" +
                "    \"include_upper\" : true,\n" +
                "    \"unit\" : \"m\",\n" +
                "    \"distance_type\" : \"sloppy_arc\",\n" +
                "    \"optimize_bbox\" : \"memory\",\n" +
                "    \"coerce\" : true,\n" +
                "    \"ignore_unmapped\" : false,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseQuery(json));
        assertTrue(e.getMessage().startsWith("Deprecated field "));
    }

    public void testFromJsonIgnoreMalformedFails() throws IOException {
        String json =
                "{\n" +
                "  \"geo_distance_range\" : {\n" +
                "    \"pin.location\" : [ -70.0, 40.0 ],\n" +
                "    \"from\" : \"200km\",\n" +
                "    \"to\" : \"400km\",\n" +
                "    \"include_lower\" : true,\n" +
                "    \"include_upper\" : true,\n" +
                "    \"unit\" : \"m\",\n" +
                "    \"distance_type\" : \"sloppy_arc\",\n" +
                "    \"optimize_bbox\" : \"memory\",\n" +
                "    \"ignore_malformed\" : true,\n" +
                "    \"ignore_unmapped\" : false,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseQuery(json));
        assertTrue(e.getMessage().startsWith("Deprecated field "));
    }

    @Override
    public void testMustRewrite() throws IOException {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        super.testMustRewrite();
    }

    public void testIgnoreUnmapped() throws IOException {
        final GeoDistanceRangeQueryBuilder queryBuilder = new GeoDistanceRangeQueryBuilder("unmapped", new GeoPoint(0.0, 0.0)).from("20m");
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(createShardContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final GeoDistanceRangeQueryBuilder failingQueryBuilder = new GeoDistanceRangeQueryBuilder("unmapped", new GeoPoint(0.0, 0.0))
                .from("20m");
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(createShardContext()));
        assertThat(e.getMessage(), containsString("failed to find geo_point field [unmapped]"));
    }
}
