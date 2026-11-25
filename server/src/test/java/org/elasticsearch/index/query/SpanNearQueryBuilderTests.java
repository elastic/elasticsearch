/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.lucene.queries.SpanMatchNoDocsQuery;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class SpanNearQueryBuilderTests extends AbstractQueryTestCase<SpanNearQueryBuilder> {
    @Override
    protected SpanNearQueryBuilder doCreateTestQueryBuilder() {
        boolean inOrder = randomBoolean();
        SpanTermQueryBuilder[] spanTermQueries = new SpanTermQueryBuilderTests().createSpanTermQueryBuilders(randomIntBetween(1, 6));
        SpanNearQueryBuilder queryBuilder = new SpanNearQueryBuilder(spanTermQueries[0], randomIntBetween(-10, 10));
        for (int i = 1; i < spanTermQueries.length; i++) {
            // Randomly add a gap queries to test special accounting for these query types
            if (inOrder && randomBoolean()) {
                // Lucene only supports gaps in ordered span_near queries
                queryBuilder.addClause(
                    new SpanNearQueryBuilder.SpanGapQueryBuilder(spanTermQueries[0].fieldName(), randomIntBetween(1, 3))
                );
            }
            queryBuilder.addClause(spanTermQueries[i]);
        }
        queryBuilder.inOrder(inOrder);
        return queryBuilder;
    }

    @Override
    protected SpanNearQueryBuilder createQueryWithInnerQuery(QueryBuilder queryBuilder) {
        assertThat(queryBuilder, instanceOf(SpanQueryBuilder.class));
        SpanNearQueryBuilder spanNear = new SpanNearQueryBuilder((SpanQueryBuilder) queryBuilder, 1);
        if (randomBoolean()) {
            // Randomly add a gap query to test special accounting for these query types
            spanNear.addClause(new SpanNearQueryBuilder.SpanGapQueryBuilder(TEXT_FIELD_NAME, randomIntBetween(1, 3)));
        }
        return spanNear;
    }

    @Override
    protected QueryBuilder createInnerQuery() {
        return new SpanTermQueryBuilder(TEXT_FIELD_NAME, "value");
    }

    @Override
    protected void doAssertLuceneQuery(SpanNearQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(
            query,
            either(instanceOf(SpanNearQuery.class)).or(instanceOf(SpanTermQuery.class))
                .or(instanceOf(SpanMatchNoDocsQuery.class))
                .or(instanceOf(MatchAllQueryBuilder.class))
        );
        if (query instanceof SpanNearQuery spanNearQuery) {
            assertThat(spanNearQuery.getSlop(), equalTo(queryBuilder.slop()));
            assertThat(spanNearQuery.isInOrder(), equalTo(queryBuilder.inOrder()));
            assertThat(spanNearQuery.getClauses().length, equalTo(queryBuilder.clauses().size()));
            Iterator<SpanQueryBuilder> spanQueryBuilderIterator = queryBuilder.clauses().iterator();
            for (SpanQuery spanQuery : spanNearQuery.getClauses()) {
                SpanQueryBuilder clause = spanQueryBuilderIterator.next();
                if (clause instanceof SpanNearQueryBuilder.SpanGapQueryBuilder gap) {
                    // SpanGapQuery is private to Lucene's SpanNearQuery, so build the expected gap through its builder
                    SpanQuery expectedGap = new SpanNearQuery.Builder(spanNearQuery.getField(), true).addGap(gap.width())
                        .build()
                        .getClauses()[0];
                    assertThat(spanQuery, equalTo(expectedGap));
                } else {
                    assertThat(spanQuery, equalTo(clause.toQuery(context)));
                }
            }
        } else if (query instanceof SpanTermQuery) {
            assertThat(queryBuilder.clauses().size(), equalTo(1));
            assertThat(query, equalTo(queryBuilder.clauses().get(0).toQuery(context)));
        }
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SpanNearQueryBuilder(null, 1));
        assertEquals("[span_near] must include at least one clause", e.getMessage());

        SpanNearQueryBuilder spanNearQueryBuilder = new SpanNearQueryBuilder(new SpanTermQueryBuilder("field", "value"), 1);
        e = expectThrows(IllegalArgumentException.class, () -> spanNearQueryBuilder.addClause(null));
        assertEquals("[span_near]  clauses cannot be null", e.getMessage());
    }

    public void testClausesUnmodifiable() {
        SpanNearQueryBuilder spanNearQueryBuilder = new SpanNearQueryBuilder(new SpanTermQueryBuilder("field", "value"), 1);
        expectThrows(
            UnsupportedOperationException.class,
            () -> spanNearQueryBuilder.clauses().add(new SpanTermQueryBuilder("field", "value2"))
        );
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
                  "span_term" : {
                    "field" : {
                      "value" : "value2"
                    }
                  }
                }, {
                  "span_term" : {
                    "field" : {
                      "value" : "value3"
                    }
                  }
                } ],
                "slop" : 12,
                "in_order" : false,
                "boost" : 2.0
              }
            }""";

        SpanNearQueryBuilder parsed = (SpanNearQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 3, parsed.clauses().size());
        assertEquals(json, 12, parsed.slop());
        assertEquals(json, false, parsed.inOrder());
        assertEquals(json, 2.0, parsed.boost(), 0.0);
    }

    public void testParsingSlopDefault() throws IOException {
        String json = """
            {
              "span_near" : {
                "clauses" : [ {
                  "span_term" : {
                    "field" : {
                      "value" : "value1",
                      "boost" : 1.0
                    }
                  }
                }]
              }
            }""";

        SpanNearQueryBuilder parsed = (SpanNearQueryBuilder) parseQuery(json);
        assertEquals(json, 1, parsed.clauses().size());
        assertEquals(json, SpanNearQueryBuilder.DEFAULT_SLOP, parsed.slop());
        assertEquals(json, SpanNearQueryBuilder.DEFAULT_BOOST, parsed.boost(), 0.0);
        assertEquals(json, SpanNearQueryBuilder.DEFAULT_IN_ORDER, parsed.inOrder());
    }

    public void testCollectPayloadsNoLongerSupported() throws Exception {
        String json = """
            {
              "span_near" : {
                "clauses" : [ {
                  "span_term" : {
                    "field" : {
                      "value" : "value1",
                      "boost" : 1.0
                    }
                  }
                }, {
                  "span_term" : {
                    "field" : {
                      "value" : "value2",
                      "boost" : 1.0
                    }
                  }
                }, {
                  "span_term" : {
                    "field" : {
                      "value" : "value3",
                      "boost" : 1.0
                    }
                  }
                } ],
                "slop" : 12,
                "in_order" : false,
                "collect_payloads" : false,
                "boost" : 1.0
              }
            }""";

        final ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(e.getMessage(), containsString("[span_near] query does not support [collect_payloads]"));
    }

    public void testFromJsonWithNonDefaultBoostInInnerQuery() {
        String json = """
            {
              "span_near" : {
                "clauses" : [ {
                  "span_term" : {
                    "field" : {
                      "value" : "value1",
                      "boost" : 2.0
                    }
                  }
                }, {
                  "span_term" : {
                    "field" : {
                      "value" : "value2",
                      "boost" : 1.0
                    }
                  }
                }, {
                  "span_term" : {
                    "field" : {
                      "value" : "value3",
                      "boost" : 1.0
                    }
                  }
                } ],
                "slop" : 12,
                "in_order" : false,
                "boost" : 1.0
              }
            }""";

        Exception exception = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(exception.getMessage(), equalTo("span_near [clauses] as a nested span clause can't have non-default boost value [2.0]"));
    }
}
