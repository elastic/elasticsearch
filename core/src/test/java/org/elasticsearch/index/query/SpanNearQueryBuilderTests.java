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
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class SpanNearQueryBuilderTests extends BaseQueryTestCase<SpanNearQueryBuilder> {

    @Override
    protected SpanNearQueryBuilder doCreateTestQueryBuilder() {
        SpanNearQueryBuilder queryBuilder = new SpanNearQueryBuilder(randomIntBetween(-10, 10));
        SpanTermQueryBuilder[] spanTermQueries = new SpanTermQueryBuilderTests().createSpanTermQueryBuilders(randomIntBetween(1, 6));
        for (SpanTermQueryBuilder clause : spanTermQueries) {
            queryBuilder.clause(clause);
        }
        queryBuilder.inOrder(randomBoolean());
        queryBuilder.collectPayloads(randomBoolean());
        return queryBuilder;
    }

    @Override
    protected void doAssertLuceneQuery(SpanNearQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(SpanNearQuery.class));
        SpanNearQuery spanNearQuery = (SpanNearQuery) query;
        assertThat(spanNearQuery.getSlop(), equalTo(queryBuilder.slop()));
        assertThat(spanNearQuery.isInOrder(), equalTo(queryBuilder.inOrder()));
        assertThat(spanNearQuery.getClauses().length, equalTo(queryBuilder.clauses().size()));
        Iterator<SpanQueryBuilder> spanQueryBuilderIterator = queryBuilder.clauses().iterator();
        for (SpanQuery spanQuery : spanNearQuery.getClauses()) {
            assertThat(spanQuery, equalTo(spanQueryBuilderIterator.next().toQuery(context)));
        }
    }

    @Test
    public void testValidate() {
        SpanNearQueryBuilder queryBuilder = new SpanNearQueryBuilder(1);
        assertValidate(queryBuilder, 1); // empty clause list

        int totalExpectedErrors = 0;
        int clauses = randomIntBetween(1, 10);
        for (int i = 0; i < clauses; i++) {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    queryBuilder.clause(new SpanTermQueryBuilder("", "test"));
                } else {
                    queryBuilder.clause(null);
                }
                totalExpectedErrors++;
            } else {
                queryBuilder.clause(new SpanTermQueryBuilder("name", "value"));
            }
        }
        assertValidate(queryBuilder, totalExpectedErrors);
    }
}
