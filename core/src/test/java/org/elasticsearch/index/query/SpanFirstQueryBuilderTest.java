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
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.junit.Test;

import java.io.IOException;

public class SpanFirstQueryBuilderTest extends BaseQueryTestCase<SpanFirstQueryBuilder> {

    @Override
    protected Query doCreateExpectedQuery(SpanFirstQueryBuilder testQueryBuilder, QueryParseContext context) throws IOException {
        SpanQuery innerQuery = (SpanQuery) testQueryBuilder.matchBuilder().toQuery(context);
        return new SpanFirstQuery(innerQuery, testQueryBuilder.end());
    }

    @Override
    protected SpanFirstQueryBuilder doCreateTestQueryBuilder() {
        SpanTermQueryBuilder innerQueryBuilder = new SpanTermQueryBuilderTest().createTestQueryBuilder();
        return new SpanFirstQueryBuilder(innerQueryBuilder, randomIntBetween(0, 1000));
    }

    @Test
    public void testValidate() {
        int totalExpectedErrors = 0;
        SpanQueryBuilder innerSpanQueryBuilder;
        if (randomBoolean()) {
            innerSpanQueryBuilder = new SpanTermQueryBuilder("", "test");
            totalExpectedErrors++;
        } else {
            innerSpanQueryBuilder = new SpanTermQueryBuilder("name", "value");
        }
        SpanFirstQueryBuilder queryBuilder = new SpanFirstQueryBuilder(innerSpanQueryBuilder, 10);
        assertValidate(queryBuilder, totalExpectedErrors);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testEndValueNegative() {
        new SpanFirstQueryBuilder(new SpanTermQueryBuilder("name", "value"), -1);
    }

    @Test(expected=NullPointerException.class)
    public void testInnerQueryNull() {
        new SpanFirstQueryBuilder(null, 1);
    }
}
