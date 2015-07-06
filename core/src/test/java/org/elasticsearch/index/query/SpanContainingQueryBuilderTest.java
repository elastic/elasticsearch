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
import org.apache.lucene.search.spans.SpanContainingQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.junit.Test;

import java.io.IOException;

public class SpanContainingQueryBuilderTest extends BaseQueryTestCase<SpanContainingQueryBuilder> {

    @Override
    protected Query doCreateExpectedQuery(SpanContainingQueryBuilder testQueryBuilder, QueryParseContext context) throws IOException {
        SpanQuery big = (SpanQuery) testQueryBuilder.big().toQuery(context);
        SpanQuery little = (SpanQuery) testQueryBuilder.little().toQuery(context);
        return new SpanContainingQuery(big, little);
    }

    @Override
    protected SpanContainingQueryBuilder doCreateTestQueryBuilder() {
        SpanTermQueryBuilder bigQuery = new SpanTermQueryBuilderTest().createTestQueryBuilder();
        // we need same field name and value type as bigQuery for little query
        String fieldName = bigQuery.fieldName();
        Object littleValue;
        switch (fieldName) {
            case BOOLEAN_FIELD_NAME: littleValue = randomBoolean(); break;
            case INT_FIELD_NAME: littleValue = randomInt(); break;
            case DOUBLE_FIELD_NAME: littleValue = randomDouble(); break;
            case STRING_FIELD_NAME: littleValue = randomAsciiOfLengthBetween(1, 10); break;
            default : littleValue = randomAsciiOfLengthBetween(1, 10);
        }
        SpanTermQueryBuilder littleQuery = new SpanTermQueryBuilder(fieldName, littleValue);
        return new SpanContainingQueryBuilder(bigQuery, littleQuery);
    }

    @Test
    public void testValidate() {
        int totalExpectedErrors = 0;
        SpanQueryBuilder bigSpanQueryBuilder;
        if (randomBoolean()) {
            bigSpanQueryBuilder = new SpanTermQueryBuilder("", "test");
            totalExpectedErrors++;
        } else {
            bigSpanQueryBuilder = new SpanTermQueryBuilder("name", "value");
        }
        SpanQueryBuilder littleSpanQueryBuilder;
        if (randomBoolean()) {
            littleSpanQueryBuilder = new SpanTermQueryBuilder("", "test");
            totalExpectedErrors++;
        } else {
            littleSpanQueryBuilder = new SpanTermQueryBuilder("name", "value");
        }
        SpanContainingQueryBuilder queryBuilder = new SpanContainingQueryBuilder(bigSpanQueryBuilder, littleSpanQueryBuilder);
        assertValidate(queryBuilder, totalExpectedErrors);
    }

    @Test(expected=NullPointerException.class)
    public void testNullBig() {
        new SpanContainingQueryBuilder(null, new SpanTermQueryBuilder("name", "value"));
    }

    @Test(expected=NullPointerException.class)
    public void testNullLittle() {
        new SpanContainingQueryBuilder(new SpanTermQueryBuilder("name", "value"), null);
    }
}
