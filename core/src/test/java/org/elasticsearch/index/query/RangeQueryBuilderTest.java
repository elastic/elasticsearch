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

import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RangeQueryBuilderTest extends BaseQueryTestCase<RangeQueryBuilder> {

    private static final List<String> TIMEZONE_IDS = new ArrayList<>(DateTimeZone.getAvailableIDs());

    @Override
    protected RangeQueryBuilder createTestQueryBuilder() {
        RangeQueryBuilder query;
        // switch between numeric and date ranges
        if (randomBoolean()) {
            if (randomBoolean()) {
                // use mapped integer field for numeric range queries
                query = new RangeQueryBuilder(INT_FIELD_NAME);
                query.from(randomIntBetween(1, 100));
                query.to(randomIntBetween(101, 200));
            } else {
                // use unmapped field for numeric range queries
                query = new RangeQueryBuilder(randomAsciiOfLengthBetween(1, 10));
                query.from(0.0-randomDouble());
                query.to(randomDouble());
            }
        } else {
            // use mapped date field, using date string representation
            query = new RangeQueryBuilder(DATE_FIELD_NAME);
            query.from(new DateTime(System.currentTimeMillis() - randomIntBetween(0, 1000000)).toString());
            query.to(new DateTime(System.currentTimeMillis() + randomIntBetween(0, 1000000)).toString());
            if (randomBoolean()) {
                query.timeZone(TIMEZONE_IDS.get(randomIntBetween(0, TIMEZONE_IDS.size()-1)));
            }
            if (randomBoolean()) {
                query.format("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
            }
        }
        query.includeLower(randomBoolean()).includeUpper(randomBoolean());
        if (randomBoolean()) {
            query.boost(2.0f / randomIntBetween(1, 20));
        }
        if (randomBoolean()) {
            query.queryName(randomAsciiOfLengthBetween(1, 10));
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
    protected Query createExpectedQuery(RangeQueryBuilder queryBuilder, QueryParseContext context) throws IOException {
        Query expectedQuery;
        String fieldName = queryBuilder.fieldName();
        if (getCurrentTypes().length == 0 || (fieldName.equals(DATE_FIELD_NAME) == false && fieldName.equals(INT_FIELD_NAME) == false) ) {
            expectedQuery = new TermRangeQuery(fieldName,
                    BytesRefs.toBytesRef(queryBuilder.from()), BytesRefs.toBytesRef(queryBuilder.to()),
                    queryBuilder.includeLower(), queryBuilder.includeUpper());

        } else if (fieldName.equals(DATE_FIELD_NAME)) {
            DateMathParser forcedDateParser = null;
            if (queryBuilder.format()  != null) {
                forcedDateParser = new DateMathParser(Joda.forPattern(queryBuilder.format()));
            }
            DateTimeZone dateTimeZone = null;
            if (queryBuilder.timeZone() != null) {
                dateTimeZone = DateTimeZone.forID(queryBuilder.timeZone());
            }
            MappedFieldType mapper = context.fieldMapper(queryBuilder.fieldName());
            expectedQuery = ((DateFieldMapper.DateFieldType) mapper).rangeQuery(queryBuilder.from(), queryBuilder.to(), queryBuilder.includeLower(), queryBuilder.includeUpper(), dateTimeZone, forcedDateParser, context);
        } else if (queryBuilder.fieldName().equals(INT_FIELD_NAME)) {
            expectedQuery = NumericRangeQuery.newIntRange(INT_FIELD_NAME, (Integer) queryBuilder.from(), (Integer) queryBuilder.to(), queryBuilder.includeLower(), queryBuilder.includeUpper());
        } else {
            throw new UnsupportedOperationException();
        }
        expectedQuery.setBoost(queryBuilder.boost());
        return expectedQuery;
    }

    @Override
    protected void assertLuceneQuery(RangeQueryBuilder queryBuilder, Query query, QueryParseContext context) {
        if (queryBuilder.queryName() != null) {
            Query namedQuery = context.copyNamedFilters().get(queryBuilder.queryName());
            assertThat(namedQuery, equalTo(query));
        }
    }

    @Test
    public void testValidate() {
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

    /**
     * Specifying a timezone together with a numeric range query should throw an error.
     */
    @Test(expected=QueryParsingException.class)
    public void testToQueryNonDateWithTimezone() throws QueryParsingException, IOException {
        RangeQueryBuilder query = new RangeQueryBuilder(INT_FIELD_NAME);
        query.from(1).to(10).timeZone("UTC");
        query.toQuery(createContext());
    }

    @Override
    protected RangeQueryBuilder createEmptyQueryBuilder() {
        return new RangeQueryBuilder(null);
    }
}
