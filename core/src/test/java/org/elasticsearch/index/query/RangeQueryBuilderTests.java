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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;

public class RangeQueryBuilderTests extends AbstractQueryTestCase<RangeQueryBuilder> {

    @Override
    protected RangeQueryBuilder doCreateTestQueryBuilder() {
        RangeQueryBuilder query;
        // switch between numeric and date ranges
        switch (randomIntBetween(0, 2)) {
            case 0:
                if (randomBoolean()) {
                    // use mapped integer field for numeric range queries
                    query = new RangeQueryBuilder(INT_FIELD_NAME);
                    query.from(randomIntBetween(1, 100));
                    query.to(randomIntBetween(101, 200));
                } else {
                    // use unmapped field for numeric range queries
                    query = new RangeQueryBuilder(randomAsciiOfLengthBetween(1, 10));
                    query.from(0.0 - randomDouble());
                    query.to(randomDouble());
                }
                break;
            case 1:
                // use mapped date field, using date string representation
                query = new RangeQueryBuilder(DATE_FIELD_NAME);
                query.from(new DateTime(System.currentTimeMillis() - randomIntBetween(0, 1000000), DateTimeZone.UTC).toString());
                query.to(new DateTime(System.currentTimeMillis() + randomIntBetween(0, 1000000), DateTimeZone.UTC).toString());
                // Create timestamp option only then we have a date mapper,
                // otherwise we could trigger exception.
                if (createShardContext().mapperService().smartNameFieldType(DATE_FIELD_NAME) != null) {
                    if (randomBoolean()) {
                        query.timeZone(randomTimeZone());
                    }
                    if (randomBoolean()) {
                        query.format("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
                    }
                }
                break;
            case 2:
            default:
                query = new RangeQueryBuilder(STRING_FIELD_NAME);
                query.from("a" + randomAsciiOfLengthBetween(1, 10));
                query.to("z" + randomAsciiOfLengthBetween(1, 10));
                break;
        }
        query.includeLower(randomBoolean()).includeUpper(randomBoolean());
        if (randomBoolean()) {
            query.from(null);
        }
        if (randomBoolean()) {
            query.to(null);
        }
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(RangeQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        if (getCurrentTypes().length == 0 || (queryBuilder.fieldName().equals(DATE_FIELD_NAME) == false && queryBuilder.fieldName().equals(INT_FIELD_NAME) == false)) {
            assertThat(query, instanceOf(TermRangeQuery.class));
        } else if (queryBuilder.fieldName().equals(DATE_FIELD_NAME)) {
            //we can't properly test unmapped dates because LateParsingQuery is package private
        } else if (queryBuilder.fieldName().equals(INT_FIELD_NAME)) {
            assertThat(query, instanceOf(NumericRangeQuery.class));
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    public void testIllegalArguments() {
        try {
            if (randomBoolean()) {
                new RangeQueryBuilder(null);
            } else {
                new RangeQueryBuilder("");
            }
            fail("cannot be null or empty");
        } catch (IllegalArgumentException e) {
            // expected
        }

        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("test");
        try {
            if (randomBoolean()) {
                rangeQueryBuilder.timeZone(null);
            } else {
                rangeQueryBuilder.timeZone("badID");
            }
            fail("cannot be null or unknown id");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            if (randomBoolean()) {
                rangeQueryBuilder.format(null);
            } else {
                rangeQueryBuilder.format("badFormat");
            }
            fail("cannot be null or bad format");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    /**
     * Specifying a timezone together with a numeric range query should throw an exception.
     */
    @Test(expected=QueryShardException.class)
    public void testToQueryNonDateWithTimezone() throws QueryShardException, IOException {
        RangeQueryBuilder query = new RangeQueryBuilder(INT_FIELD_NAME);
        query.from(1).to(10).timeZone("UTC");
        query.toQuery(createShardContext());
    }

    /**
     * Specifying a timezone together with an unmapped field should throw an exception.
     */
    @Test(expected=QueryShardException.class)
    public void testToQueryUnmappedWithTimezone() throws QueryShardException, IOException {
        RangeQueryBuilder query = new RangeQueryBuilder("bogus_field");
        query.from(1).to(10).timeZone("UTC");
        query.toQuery(createShardContext());
    }
}
