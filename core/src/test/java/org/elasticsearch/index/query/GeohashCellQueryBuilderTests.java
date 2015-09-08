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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.query.GeohashCellQuery.Builder;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class GeohashCellQueryBuilderTests extends BaseQueryTestCase<GeohashCellQuery.Builder> {

    @Override
    protected Builder doCreateTestQueryBuilder() {
        GeohashCellQuery.Builder builder = new Builder(GEO_FIELD_NAME);
        builder.geohash(randomGeohash(1, 12));
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

    @Test
    public void testNullField() {
        GeohashCellQuery.Builder builder = new Builder(null);
        builder.geohash(randomGeohash(1, 12));
        QueryValidationException exception = builder.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), notNullValue());
        assertThat(exception.validationErrors().size(), equalTo(1));
        assertThat(exception.validationErrors().get(0), equalTo("[" + GeohashCellQuery.NAME + "] fieldName must not be null"));
    }

    @Test
    public void testNullGeohash() {
        GeohashCellQuery.Builder builder = new Builder(GEO_FIELD_NAME);
        QueryValidationException exception = builder.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), notNullValue());
        assertThat(exception.validationErrors().size(), equalTo(1));
        assertThat(exception.validationErrors().get(0), equalTo("[" + GeohashCellQuery.NAME + "] geohash or point must be defined"));
    }

    @Test
    public void testInvalidPrecision() {
        GeohashCellQuery.Builder builder = new Builder(GEO_FIELD_NAME);
        builder.geohash(randomGeohash(1, 12));
        builder.precision(-1);
        QueryValidationException exception = builder.validate();
        assertThat(exception, notNullValue());
        assertThat(exception.validationErrors(), notNullValue());
        assertThat(exception.validationErrors().size(), equalTo(1));
        assertThat(exception.validationErrors().get(0), equalTo("[" + GeohashCellQuery.NAME + "] precision must be greater than 0. Found ["
                + -1 + "]"));
    }

}
