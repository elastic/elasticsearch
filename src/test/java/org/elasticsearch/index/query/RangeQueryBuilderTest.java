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
import org.apache.lucene.search.TermRangeQuery;
import org.elasticsearch.common.lucene.BytesRefs;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class RangeQueryBuilderTest extends BaseQueryTestCase<RangeQueryBuilder> {

    private static final List<String> TIMEZONE_IDS = new ArrayList<>(DateTimeZone.getAvailableIDs());

    @Override
    protected RangeQueryBuilder createTestQueryBuilder() {
        RangeQueryBuilder query = new RangeQueryBuilder(randomAsciiOfLength(8));
        if (randomBoolean()) {
            query.boost(2.0f / randomIntBetween(1, 20));
        }
        if (randomBoolean()) {
            query.queryName(randomAsciiOfLength(8));
        }
        query.includeLower(randomBoolean()).includeUpper(randomBoolean());
        // switch between numeric and date ranges
        if (randomBoolean()) {
            query.from(randomIntBetween(1, 100));
            query.to(randomIntBetween(101, 200));
        } else {
            query.from(new DateTime(System.currentTimeMillis() - randomIntBetween(0, 100000)).toString());
            query.from(new DateTime(System.currentTimeMillis() + randomIntBetween(0, 100000)).toString());
            if (randomBoolean()) {
                query.timeZone(TIMEZONE_IDS.get(randomIntBetween(0, TIMEZONE_IDS.size())));
            }
            if (randomBoolean()) {
                query.format("yyyy-MM-ddTHH:mm:ss.SSSZZ");
            }
        }

        if (randomBoolean()) {
            query.from(null);
        }
        if (randomBoolean()) {
            query.to(null);
        }
        return query;
    }

    @Override
    protected void assertLuceneQuery(RangeQueryBuilder queryBuilder, Query query, QueryParseContext context) throws IOException {
        assertThat(query, instanceOf(TermRangeQuery.class));
        assertThat(query.getBoost(), is(queryBuilder.boost()));
        TermRangeQuery termRangeQuery = (TermRangeQuery) query;
        assertThat(termRangeQuery.includesLower(), is(queryBuilder.includeLower()));
        assertThat(termRangeQuery.includesUpper(), is(queryBuilder.includeUpper()));
        assertThat(termRangeQuery.getLowerTerm(), is(BytesRefs.toBytesRef(queryBuilder.from())));
        assertThat(termRangeQuery.getUpperTerm(), is(BytesRefs.toBytesRef(queryBuilder.to())));
        assertThat(termRangeQuery.getField(), is(queryBuilder.fieldname()));
        if (queryBuilder.queryName() != null) {
            Query namedQuery = context.copyNamedFilters().get(queryBuilder.queryName());
            assertThat(namedQuery, equalTo((Query) termRangeQuery));
        }
    }

    @Test
    public void testValidation() {
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("");
        assertThat(rangeQueryBuilder.validate().validationErrors().size(), is(1));

        rangeQueryBuilder = new RangeQueryBuilder("okay").timeZone("UTC");
        assertNull(rangeQueryBuilder.validate());

        rangeQueryBuilder.timeZone("blab");
        assertThat(rangeQueryBuilder.validate().validationErrors().size(), is(1));

        rangeQueryBuilder.timeZone("UTC").format("basicDate");
        assertNull(rangeQueryBuilder.validate());

        rangeQueryBuilder.timeZone("UTC").format("broken_xx");
        assertThat(rangeQueryBuilder.validate().validationErrors().size(), is(1));

        rangeQueryBuilder.timeZone("xXx").format("broken_xx");
        assertThat(rangeQueryBuilder.validate().validationErrors().size(), is(2));
    }

    @Override
    protected RangeQueryBuilder createEmptyQueryBuilder() {
        return new RangeQueryBuilder();
    }

}
