/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.query.DistanceFeatureQueryBuilder.Origin;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.time.Instant;

import static org.hamcrest.Matchers.containsString;

public class DistanceFeatureQueryBuilderTests extends AbstractQueryTestCase<DistanceFeatureQueryBuilder> {
    @Override
    protected DistanceFeatureQueryBuilder doCreateTestQueryBuilder() {
        String field = randomFrom(DATE_FIELD_NAME, DATE_NANOS_FIELD_NAME, GEO_POINT_FIELD_NAME);
        Origin origin;
        String pivot;
        switch (field) {
            case GEO_POINT_FIELD_NAME:
                GeoPoint point = new GeoPoint(randomDouble(), randomDouble());
                origin = randomBoolean() ? new Origin(point) : new Origin(point.geohash());
                pivot = randomFrom(DistanceUnit.values()).toString(randomDouble());
                break;
            case DATE_FIELD_NAME:
                long randomDateMills = randomLongBetween(0, 2_000_000_000_000L);
                origin = randomBoolean() ? new Origin(randomDateMills) : new Origin(Instant.ofEpochMilli(randomDateMills).toString());
                pivot = randomTimeValue(1, 1000, "d", "h", "ms", "s", "m");
                break;
            default: // DATE_NANOS_FIELD_NAME
                randomDateMills = randomLongBetween(0, 2_000_000_000_000L);
                if (randomBoolean()) {
                    origin = new Origin(randomDateMills); // nano_dates long accept milliseconds since epoch
                } else {
                    long randomNanos = randomLongBetween(0, 1_000_000L);
                    Instant randomDateNanos = Instant.ofEpochMilli(randomDateMills).plusNanos(randomNanos);
                    origin = new Origin(randomDateNanos.toString());
                }
                pivot = randomTimeValue(1, 100_000_000, "nanos");
                break;
        }
        return new DistanceFeatureQueryBuilder(field, origin, pivot);
    }

    @Override
    protected void doAssertLuceneQuery(DistanceFeatureQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        String fieldName = expectedFieldName(queryBuilder.fieldName());
        Object origin = queryBuilder.origin().origin();
        String pivot = queryBuilder.pivot();
        final Query expectedQuery;
        if (fieldName.equals(GEO_POINT_FIELD_NAME)) {
            GeoPoint originGeoPoint = (origin instanceof GeoPoint) ? (GeoPoint) origin : GeoUtils.parseFromString((String) origin);
            double pivotDouble = DistanceUnit.DEFAULT.parse(pivot, DistanceUnit.DEFAULT);
            expectedQuery = LatLonPoint.newDistanceFeatureQuery(fieldName, 1.0f, originGeoPoint.lat(), originGeoPoint.lon(), pivotDouble);
        } else { // if (fieldName.equals(DATE_FIELD_NAME))
            DateFieldType fieldType = (DateFieldType) context.getFieldType(fieldName);
            long originLong = fieldType.parseToLong(origin, true, null, null, context::nowInMillis);
            TimeValue pivotVal = TimeValue.parseTimeValue(pivot, DistanceFeatureQueryBuilder.class.getSimpleName() + ".pivot");
            long pivotLong;
            if (fieldType.resolution() == DateFieldMapper.Resolution.MILLISECONDS) {
                pivotLong = pivotVal.getMillis();
            } else { // NANOSECONDS
                pivotLong = pivotVal.getNanos();
            }
            expectedQuery = LongPoint.newDistanceFeatureQuery(fieldName, 1.0f, originLong, pivotLong);
        }
        assertEquals(expectedQuery, query);
    }

    public void testFromJsonDateFieldType() throws IOException {
        // origin as string
        String origin = "2018-01-01T13:10:30Z";
        String pivot = "7d";
        String json = """
            {
              "distance_feature": {
                "field": "%s",
                "origin": "%s",
                "pivot": "%s",
                "boost": 1.0
              }
            }""".formatted(DATE_FIELD_NAME, origin, pivot);
        DistanceFeatureQueryBuilder parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, origin, parsed.origin().origin());
        assertEquals(json, pivot, parsed.pivot());
        assertEquals(json, 1.0, parsed.boost(), 0.0001);

        // origin as long
        long originLong = 1514812230999L;
        json = """
            {
              "distance_feature": {
                "field": "%s",
                "origin": %s,
                "pivot": "%s",
                "boost": 1.0
              }
            }""".formatted(DATE_FIELD_NAME, originLong, pivot);
        parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        assertEquals(json, originLong, parsed.origin().origin());
    }

    public void testFromJsonDateNanosFieldType() throws IOException {
        // origin as string
        String origin = "2018-01-01T13:10:30.323456789Z";
        String pivot = "100000000nanos";
        String json = """
            {
              "distance_feature": {
                "field": "%s",
                "origin": "%s",
                "pivot": "%s",
                "boost": 1.0
              }
            }""".formatted(DATE_NANOS_FIELD_NAME, origin, pivot);
        DistanceFeatureQueryBuilder parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, origin, parsed.origin().origin());
        assertEquals(json, pivot, parsed.pivot());
        assertEquals(json, 1.0, parsed.boost(), 0.0001);

        // origin as long
        long originLong = 1514812230999L;
        json = """
            {
              "distance_feature": {
                "field": "%s",
                "origin": %s,
                "pivot": "%s",
                "boost": 1.0
              }
            }""".formatted(DATE_NANOS_FIELD_NAME, originLong, pivot);
        parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        assertEquals(json, originLong, parsed.origin().origin());
    }

    public void testFromJsonGeoFieldType() throws IOException {
        final GeoPoint origin = new GeoPoint(41.12, -71.34);
        final String pivot = "1km";

        // origin as string
        String json = """
            {
              "distance_feature": {
                "field": "%s",
                "origin": "%s",
                "pivot": "%s",
                "boost": 2.0
              }
            }""".formatted(GEO_POINT_FIELD_NAME, origin.toString(), pivot);
        DistanceFeatureQueryBuilder parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, origin.toString(), parsed.origin().origin());
        assertEquals(json, pivot, parsed.pivot());
        assertEquals(json, 2.0, parsed.boost(), 0.0001);

        // origin as array
        json = """
            {
              "distance_feature": {
                "field": "%s",
                "origin": [ %s, %s ],
                "pivot": "%s",
                "boost": 2.0
              }
            }""".formatted(GEO_POINT_FIELD_NAME, origin.lon(), origin.lat(), pivot);
        parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        assertEquals(json, origin, parsed.origin().origin());

        // origin as object
        json = """
            {
              "distance_feature": {
                "field": "%s",
                "origin": {
                  "lat": %s,
                  "lon": %s
                },
                "pivot": "%s",
                "boost": 2.0
              }
            }""".formatted(GEO_POINT_FIELD_NAME, origin.lat(), origin.lon(), pivot);
        parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        assertEquals(json, origin, parsed.origin().origin());
    }

    public void testQueryMatchNoDocsQueryWithUnmappedField() throws IOException {
        Query expectedQuery = Queries.newMatchNoDocsQuery("Can't run [" + DistanceFeatureQueryBuilder.NAME + "] query on unmapped fields!");
        String queryString = """
            {
                "distance_feature" : {
                        "field": "random_unmapped_field",
                        "origin": "random_string",
                        "pivot" : "random_string"
                }
            }""";
        Query query = parseQuery(queryString).toQuery(createSearchExecutionContext());
        assertEquals(expectedQuery, query);
    }

    public void testQueryFailsWithWrongFieldType() {
        String query = """
            {
              "distance_feature": {
                "field": "%s",
                "origin": 40,
                "pivot": "random_string"
              }
            }""".formatted(INT_FIELD_NAME);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> parseQuery(query).toQuery(createSearchExecutionContext())
        );
        assertThat(e.getMessage(), containsString("query can only be run on a date, date_nanos or geo_point field type!"));
    }
}
