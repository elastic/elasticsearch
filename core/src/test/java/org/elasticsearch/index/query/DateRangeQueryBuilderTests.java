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

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.joda.time.DateTime;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;

public class DateRangeQueryBuilderTests extends AbstractQueryTestCase<DateRangeQueryBuilder> {

    public void testDateRangeBoundaries() throws IOException {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        String query = "{\n" +
            "    \"range\" : {\n" +
            "        \"" + DATE_FIELD_NAME + "\" : {\n" +
            "            \"gte\": \"2014-11-05||/M\",\n" +
            "            \"lte\": \"2014-12-08||/d\"\n" +
            "        }\n" +
            "    }\n" +
            "}";
        Query parsedQuery = parseQuery(query).toQuery(createShardContext());
        assertThat(parsedQuery, instanceOf(PointRangeQuery.class));
        assertEquals(LongPoint.newRangeQuery(DATE_FIELD_NAME,
            DateTime.parse("2014-11-01T00:00:00.000+00").getMillis(),
            DateTime.parse("2014-12-08T23:59:59.999+00").getMillis()),
            parsedQuery);

        query = "{\n" +
            "    \"range\" : {\n" +
            "        \"" + DATE_FIELD_NAME + "\" : {\n" +
            "            \"gt\": \"2014-11-05||/M\",\n" +
            "            \"lt\": \"2014-12-08||/d\"\n" +
            "        }\n" +
            "    }\n" +
            "}";
        parsedQuery = parseQuery(query).toQuery(createShardContext());
        assertThat(parsedQuery, instanceOf(PointRangeQuery.class));
        assertEquals(LongPoint.newRangeQuery(DATE_FIELD_NAME,
            DateTime.parse("2014-11-30T23:59:59.999+00").getMillis() + 1,
            DateTime.parse("2014-12-08T00:00:00.000+00").getMillis() - 1),
            parsedQuery);
    }

    @Override
    protected DateRangeQueryBuilder doCreateTestQueryBuilder() {
        DateRangeQueryBuilder query;

        // use mapped date field, using date string representation
        query = new DateRangeQueryBuilder(randomBoolean() ? DATE_FIELD_NAME : DATE_RANGE_FIELD_NAME);
        query.gt(new DateTime(System.currentTimeMillis() - randomIntBetween(0, 1000000)));
        query.gte(new DateTime(System.currentTimeMillis() - randomIntBetween(0, 1000000)));
        query.lt(new DateTime(System.currentTimeMillis() + randomIntBetween(0, 1000000)));
        query.lte(new DateTime(System.currentTimeMillis() - randomIntBetween(0, 1000000)));
        // Create timestamp option only then we have a date mapper,
        // otherwise we could trigger exception.
        if (createShardContext().getMapperService().fullName(DATE_FIELD_NAME) != null) {
            if (randomBoolean()) {
                query.timeZone(randomDateTimeZone().getID());
            }
            if (randomBoolean()) {
                query.format("yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
            }
        }
        if (query.fieldName().equals(DATE_RANGE_FIELD_NAME)) {
            query.relation(RandomPicks.randomFrom(random(), ShapeRelation.values()).getRelationName());
        }

        return query;
    }

    @Override
    protected void doAssertLuceneQuery(DateRangeQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {

    }
}
