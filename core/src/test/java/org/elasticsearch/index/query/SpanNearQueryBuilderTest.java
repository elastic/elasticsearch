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
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class SpanNearQueryBuilderTest extends BaseQueryTestCase<SpanNearQueryBuilder> {

    @Override
    protected Query doCreateExpectedQuery(SpanNearQueryBuilder testQueryBuilder, QueryParseContext context) throws IOException {
        List<SpanQueryBuilder> clauses = testQueryBuilder.clauses();
        SpanQuery[] spanQueries = new SpanQuery[clauses.size()];
        for (int i = 0; i < clauses.size(); i++) {
            Query query = clauses.get(i).toQuery(context);
            assert query instanceof SpanQuery;
            spanQueries[i] = (SpanQuery) query;
        }
        return new SpanNearQuery(spanQueries, testQueryBuilder.slop(), testQueryBuilder.inOrder(), testQueryBuilder.collectPayloads());

    }

    @Override
    protected SpanNearQueryBuilder doCreateTestQueryBuilder() {
        SpanNearQueryBuilder queryBuilder = new SpanNearQueryBuilder(randomIntBetween(-10, 10));
        int clauses = randomIntBetween(1, 6);
        // we use one random SpanTermQueryBuilder to determine same field name for subsequent clauses
        String fieldName = new SpanTermQueryBuilderTest().createTestQueryBuilder().fieldName();
        for (int i = 0; i < clauses; i++) {
            // we need same field name in all clauses, so we only randomize value
            queryBuilder.clause(new SpanTermQueryBuilder(fieldName, randomValueForField(fieldName)));
        }
        queryBuilder.inOrder(randomBoolean());
        queryBuilder.collectPayloads(randomBoolean());
        return queryBuilder;
    }

    @Test
    public void testValidate() {
        SpanNearQueryBuilder queryBuilder = new SpanNearQueryBuilder(1);
        assertValidate(queryBuilder, 1); // empty clause list

        int totalExpectedErrors = 0;
        int clauses = randomIntBetween(1, 10);
        for (int i = 0; i < clauses; i++) {
            if (randomBoolean()) {
                queryBuilder.clause(new SpanTermQueryBuilder("", "test"));
                totalExpectedErrors++;
            } else {
                queryBuilder.clause(new SpanTermQueryBuilder("name", "value"));
            }
        }
        assertValidate(queryBuilder, totalExpectedErrors);
    }
}
