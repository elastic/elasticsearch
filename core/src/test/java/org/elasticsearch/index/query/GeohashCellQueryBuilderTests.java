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

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.mapper.geo.BaseGeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.query.GeohashCellQuery.Builder;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.locationtech.spatial4j.shape.Point;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.equalTo;

public class GeohashCellQueryBuilderTests extends AbstractQueryTestCase<Builder> {

    @Override
    protected Builder doCreateTestQueryBuilder() {
        GeohashCellQuery.Builder builder = new Builder(GEO_POINT_FIELD_NAME, randomGeohash(1, 12));
        if (randomBoolean()) {
            builder.neighbors(randomBoolean());
        }
        if (randomBoolean()) {
            if (randomBoolean()) {
                builder.precision(randomIntBetween(1, 12));
            } else {
                builder.precision(randomIntBetween(1, 1000000) + randomFrom(DistanceUnit.values()).toString());
            }
        }
        if (randomBoolean()) {
            builder.ignoreUnmapped(randomBoolean());
        }
        return builder;
    }

    @Override
    protected void doAssertLuceneQuery(Builder queryBuilder, Query query, QueryShardContext context) throws IOException {
        if (queryBuilder.neighbors()) {
            assertThat(query, instanceOf(TermsQuery.class));
        } else {
            assertThat(query, instanceOf(TermQuery.class));
            TermQuery termQuery = (TermQuery) query;
            Term term = termQuery.getTerm();
            assertThat(term.field(), equalTo(queryBuilder.fieldName() + "." + GeoPointFieldMapper.Names.GEOHASH));
            String geohash = queryBuilder.geohash();
            if (queryBuilder.precision() != null) {
                int len = Math.min(queryBuilder.precision(), geohash.length());
                geohash = geohash.substring(0, len);
            }
            assertThat(term.text(), equalTo(geohash));
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

    public void testNullField() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new Builder(null, new GeoPoint()));
        assertEquals("fieldName must not be null", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new Builder("", new GeoPoint()));
        assertEquals("fieldName must not be null", e.getMessage());
    }

    public void testNullGeoPoint() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new Builder(GEO_POINT_FIELD_NAME, (GeoPoint) null));
        assertEquals("geohash or point must be defined", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new Builder(GEO_POINT_FIELD_NAME, ""));
        assertEquals("geohash or point must be defined", e.getMessage());
    }

    public void testInvalidPrecision() {
        GeohashCellQuery.Builder builder = new Builder(GEO_POINT_FIELD_NAME, new GeoPoint());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.precision(-1));
        assertThat(e.getMessage(), containsString("precision must be greater than 0"));
    }

    public void testLocationParsing() throws IOException {
        Point point = RandomShapeGenerator.xRandomPoint(random());
        Builder pointTestBuilder = new GeohashCellQuery.Builder("pin", new GeoPoint(point.getY(), point.getX()));
        String pointTest1 = "{\"geohash_cell\": {\"pin\": {\"lat\": " + point.getY() + ",\"lon\": " + point.getX() + "}}}";
        assertParsedQuery(pointTest1, pointTestBuilder);
        String pointTest2 = "{\"geohash_cell\": {\"pin\": \"" + point.getY() + "," + point.getX() + "\"}}";
        assertParsedQuery(pointTest2, pointTestBuilder);
        String pointTest3 = "{\"geohash_cell\": {\"pin\": [" + point.getX() + "," + point.getY() + "]}}";
        assertParsedQuery(pointTest3, pointTestBuilder);
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"geohash_cell\" : {\n" +
                "    \"neighbors\" : true,\n" +
                "    \"precision\" : 3,\n" +
                "    \"pin\" : \"t4mk70fgk067\",\n" +
                "    \"ignore_unmapped\" : false,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";
        GeohashCellQuery.Builder parsed = (GeohashCellQuery.Builder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, 3, parsed.precision().intValue());
    }

    @Override
    public void testMustRewrite() throws IOException {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        super.testMustRewrite();
    }

    public void testIgnoreUnmapped() throws IOException {
        final GeohashCellQuery.Builder queryBuilder = new GeohashCellQuery.Builder("unmapped", "c");
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(createShardContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final GeohashCellQuery.Builder failingQueryBuilder = new GeohashCellQuery.Builder("unmapped", "c");
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(createShardContext()));
        assertThat(e.getMessage(), containsString("failed to parse [" + GeohashCellQuery.NAME + "] query. missing ["
                + BaseGeoPointFieldMapper.CONTENT_TYPE + "] field [unmapped]"));
    }
}
