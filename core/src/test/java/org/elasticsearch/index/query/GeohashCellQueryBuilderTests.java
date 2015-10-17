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

import com.spatial4j.core.shape.Point;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.query.GeohashCellQuery.Builder;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

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
            assertThat(term.field(), equalTo(queryBuilder.fieldName() + GeoPointFieldMapper.Names.GEOHASH_SUFFIX));
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

    @Test(expected=IllegalArgumentException.class)
    public void testNullField() {
        if (randomBoolean()) {
            new Builder(null, new GeoPoint());
        } else {
            new Builder("", new GeoPoint());
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testNullGeoPoint() {
        if (randomBoolean()) {
            new Builder(GEO_POINT_FIELD_NAME, (GeoPoint) null);
        } else {
            new Builder(GEO_POINT_FIELD_NAME, "");
        }
    }

    @Test(expected=IllegalArgumentException.class)
    public void testInvalidPrecision() {
        GeohashCellQuery.Builder builder = new Builder(GEO_POINT_FIELD_NAME, new GeoPoint());
        builder.precision(-1);
    }

    @Test
    public void testLocationParsing() throws IOException {
        Point point = RandomShapeGenerator.xRandomPoint(getRandom());
        Builder pointTestBuilder = new GeohashCellQuery.Builder("pin", new GeoPoint(point.getY(), point.getX()));
        String pointTest1 = "{\"geohash_cell\": {\"pin\": {\"lat\": " + point.getY() + ",\"lon\": " + point.getX() + "}}}";
        assertParsedQuery(pointTest1, pointTestBuilder);
        String pointTest2 = "{\"geohash_cell\": {\"pin\": \"" + point.getY() + "," + point.getX() + "\"}}";
        assertParsedQuery(pointTest2, pointTestBuilder);
        String pointTest3 = "{\"geohash_cell\": {\"pin\": [" + point.getX() + "," + point.getY() + "]}}";
        assertParsedQuery(pointTest3, pointTestBuilder);
    }
}
