/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Iterator;

import static org.elasticsearch.index.query.SpanNearQueryBuilder.SpanGapQueryBuilder;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

/*
 * SpanGapQueryBuilder, unlike other QBs, is not used to build a Query. Therefore, it is not suited
 * to test pattern of AbstractQueryTestCase. Since it is only used in SpanNearQueryBuilder, its test cases
 * are same as those of later with SpanGapQueryBuilder included as clauses.
 */

public class SpanGapQueryBuilderTests extends AbstractQueryTestCase<SpanNearQueryBuilder> {
    @Override
    protected SpanNearQueryBuilder doCreateTestQueryBuilder() {
        SpanTermQueryBuilder[] spanTermQueries = new SpanTermQueryBuilderTests().createSpanTermQueryBuilders(randomIntBetween(1, 6));
        SpanNearQueryBuilder queryBuilder = new SpanNearQueryBuilder(spanTermQueries[0], randomIntBetween(-10, 10));
        for (int i = 1; i < spanTermQueries.length; i++) {
            SpanTermQueryBuilder termQB = spanTermQueries[i];
            queryBuilder.addClause(termQB);
            if (i % 2 == 1) {
                SpanGapQueryBuilder gapQB = new SpanGapQueryBuilder(termQB.fieldName(), randomIntBetween(1, 2));
                queryBuilder.addClause(gapQB);
            }
        }
        queryBuilder.inOrder(true);
        return queryBuilder;
    }

    @Override
    protected void doAssertLuceneQuery(SpanNearQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(
            query,
            either(instanceOf(SpanNearQuery.class)).or(instanceOf(SpanTermQuery.class)).or(instanceOf(MatchAllQueryBuilder.class))
        );
        if (query instanceof SpanNearQuery spanNearQuery) {
            assertThat(spanNearQuery.getSlop(), equalTo(queryBuilder.slop()));
            assertThat(spanNearQuery.isInOrder(), equalTo(queryBuilder.inOrder()));
            assertThat(spanNearQuery.getClauses().length, equalTo(queryBuilder.clauses().size()));
            Iterator<SpanQueryBuilder> spanQueryBuilderIterator = queryBuilder.clauses().iterator();
            for (SpanQuery spanQuery : spanNearQuery.getClauses()) {
                SpanQueryBuilder spanQB = spanQueryBuilderIterator.next();
                if (spanQB instanceof SpanGapQueryBuilder) continue;
                assertThat(spanQuery, equalTo(spanQB.toQuery(context)));
            }
        } else if (query instanceof SpanTermQuery) {
            assertThat(queryBuilder.clauses().size(), equalTo(1));
            assertThat(query, equalTo(queryBuilder.clauses().get(0).toQuery(context)));
        }
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SpanGapQueryBuilder(null, 1));
        assertEquals("[span_gap] field name is null or empty", e.getMessage());
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "span_near" : {
                "clauses" : [ {
                  "span_term" : {
                    "field" : {
                      "value" : "value1"
                    }
                  }
                }, {
                  "span_gap" : {
                    "field" : 2      }
                }, {
                  "span_term" : {
                    "field" : {
                      "value" : "value3"
                    }
                  }
                } ],
                "slop" : 12,
                "in_order" : false
              }
            }""";

        SpanNearQueryBuilder parsed = (SpanNearQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 3, parsed.clauses().size());
        assertEquals(json, 12, parsed.slop());
        assertEquals(json, false, parsed.inOrder());
    }
}
