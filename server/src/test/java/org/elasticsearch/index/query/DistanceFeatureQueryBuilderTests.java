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

import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.joda.time.DateTime;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class DistanceFeatureQueryBuilderTests extends AbstractQueryTestCase<DistanceFeatureQueryBuilder> {
    @Override
    protected DistanceFeatureQueryBuilder doCreateTestQueryBuilder() {
        String field = randomFrom(LONG_FIELD_NAME, DATE_FIELD_NAME, GEO_POINT_FIELD_NAME);
        Object origin;
        Object pivot;
        switch (field) {
            case GEO_POINT_FIELD_NAME:
                origin = new GeoPoint(randomDouble(), randomDouble());
                origin = randomBoolean()? origin : ((GeoPoint) origin).geohash();
                pivot = randomFrom(DistanceUnit.values()).toString(randomDouble());
                break;
            case DATE_FIELD_NAME:
                long randomDateMills = System.currentTimeMillis() - randomIntBetween(0, 1000000);
                origin = randomBoolean() ? randomDateMills : new DateTime(randomDateMills).toString();
                pivot = randomTimeValue(1, 1000, "d", "h", "ms", "s", "m");
                break;
            default: // LONG_FIELD_NAME
                origin = randomLongBetween(1, Long.MAX_VALUE);
                pivot = randomLongBetween(1, Long.MAX_VALUE);
                break;
        }
        return new DistanceFeatureQueryBuilder(field, origin, pivot);
    }

    @Override
    protected void doAssertLuceneQuery(DistanceFeatureQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        String fieldName = expectedFieldName(queryBuilder.fieldName());
        Object origin = queryBuilder.origin();
        Object pivot = queryBuilder.pivot();
        float boost = queryBuilder.boost;
        final Query expectedQuery;
        if (fieldName.equals(GEO_POINT_FIELD_NAME)) {
            GeoPoint originGeoPoint = (origin instanceof GeoPoint)? (GeoPoint) origin : GeoUtils.parseFromString((String) origin);
            double pivotDouble = DistanceUnit.DEFAULT.parse((String) pivot, DistanceUnit.DEFAULT);
            expectedQuery = LatLonPoint.newDistanceFeatureQuery(fieldName, boost, originGeoPoint.lat(), originGeoPoint.lon(), pivotDouble);
        } else if (fieldName.equals(DATE_FIELD_NAME)) {
            MapperService mapperService = context.getQueryShardContext().getMapperService();
            MappedFieldType fieldType = mapperService.fullName(fieldName);
            long originLong = (origin instanceof Long) ? (Long) origin :
                ((DateFieldMapper.DateFieldType) fieldType).parseToLong(origin, true, null, null, context.getQueryShardContext());
            TimeValue val = TimeValue.parseTimeValue((String)pivot, TimeValue.timeValueHours(24),
                DistanceFeatureQueryBuilder.class.getSimpleName() + ".pivot");
            long pivotLong = val.getMillis();
            expectedQuery = LongPoint.newDistanceFeatureQuery(fieldName, boost, originLong, pivotLong);
        } else {
            long originLong = (Long) origin;
            long pivotLong = (Long) pivot;
            expectedQuery = LongPoint.newDistanceFeatureQuery(fieldName, boost, originLong, pivotLong);
        }
        assertEquals(expectedQuery, query);
    }

    public void testFromJsonLongFieldType() throws IOException {
        String json = "{\n" +
            "    \"distance_feature\" : {\n" +
            "            \"field\": \""+ LONG_FIELD_NAME + "\",\n" +
            "            \"origin\": 40,\n" +
            "            \"pivot\" : 10,\n" +
            "            \"boost\" : 2.0\n" +
            "    }\n" +
            "}";
        DistanceFeatureQueryBuilder parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, 40L, parsed.origin());
        assertEquals(json, 10L, parsed.pivot());
        assertEquals(json, 2.0, parsed.boost(), 0.0001);
    }

    public void testFromJsonDateFieldType() throws IOException {
        // origin as string
        String origin = "2018-01-01T13:10:30Z";
        String pivot = "7d";
        String json = "{\n" +
            "    \"distance_feature\" : {\n" +
            "            \"field\": \""+ DATE_FIELD_NAME + "\",\n" +
            "            \"origin\": \"" + origin + "\",\n" +
            "            \"pivot\" : \"" + pivot + "\",\n" +
            "            \"boost\" : 1.0\n" +
            "    }\n" +
            "}";
        DistanceFeatureQueryBuilder parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, origin, parsed.origin());
        assertEquals(json, pivot, parsed.pivot());
        assertEquals(json, 1.0, parsed.boost(), 0.0001);

        // origin as long
        long originLong = 1514812230999L;
        json = "{\n" +
            "    \"distance_feature\" : {\n" +
            "            \"field\": \""+ DATE_FIELD_NAME + "\",\n" +
            "            \"origin\": " + originLong + ",\n" +
            "            \"pivot\" : \"" + pivot + "\",\n" +
            "            \"boost\" : 1.0\n" +
            "    }\n" +
            "}";
        parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        assertEquals(json, originLong, parsed.origin());
    }

    public void testFromJsonGeoFieldType() throws IOException {
        final GeoPoint origin = new GeoPoint(41.12,-71.34);
        final String pivot = "1km";

        // origin as string
        String json = "{\n" +
            "    \"distance_feature\" : {\n" +
            "            \"field\": \""+ GEO_POINT_FIELD_NAME + "\",\n" +
            "            \"origin\": \"" + origin.toString() + "\",\n" +
            "            \"pivot\" : \"" + pivot + "\",\n" +
            "            \"boost\" : 2.0\n" +
            "    }\n" +
            "}";
        DistanceFeatureQueryBuilder parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, origin.toString(), parsed.origin());
        assertEquals(json, pivot, parsed.pivot());
        assertEquals(json, 2.0, parsed.boost(), 0.0001);

        // origin as array
        json = "{\n" +
            "    \"distance_feature\" : {\n" +
            "            \"field\": \""+ GEO_POINT_FIELD_NAME + "\",\n" +
            "            \"origin\": [" + origin.lon() + ", " + origin.lat() + "],\n" +
            "            \"pivot\" : \"" + pivot + "\",\n" +
            "            \"boost\" : 2.0\n" +
            "    }\n" +
            "}";
        parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        assertEquals(json, origin, parsed.origin());

        // origin as object
        json = "{\n" +
            "    \"distance_feature\" : {\n" +
            "            \"field\": \""+ GEO_POINT_FIELD_NAME + "\",\n" +
            "            \"origin\": {" + "\"lat\":"+ origin.lat() + ", \"lon\":"+ origin.lon() + "},\n" +
            "            \"pivot\" : \"" + pivot + "\",\n" +
            "            \"boost\" : 2.0\n" +
            "    }\n" +
            "}";
        parsed = (DistanceFeatureQueryBuilder) parseQuery(json);
        assertEquals(json, origin, parsed.origin());
    }

    public void testQueryFailsWithUnmappedField() {
        String query = "{\n" +
            "    \"distance_feature\" : {\n" +
            "            \"field\": \"random_unmapped_field\",\n" +
            "            \"origin\": 40,\n" +
            "            \"pivot\" : 10\n" +
            "    }\n" +
            "}";
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseQuery(query).toQuery(createShardContext()));
        assertEquals("Can't run [" + DistanceFeatureQueryBuilder.NAME + "] query on unmapped fields!", e.getMessage());
    }

    public void testQueryFailsWithWrongFieldType() {
        String query = "{\n" +
            "    \"distance_feature\" : {\n" +
            "            \"field\": \""+ INT_FIELD_NAME + "\",\n" +
            "            \"origin\": 40,\n" +
            "            \"pivot\" : 10\n" +
            "    }\n" +
            "}";
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseQuery(query).toQuery(createShardContext()));
        assertThat(e.getMessage(), containsString("query can only be run on a long, date or geo-point data type!"));
    }
}
