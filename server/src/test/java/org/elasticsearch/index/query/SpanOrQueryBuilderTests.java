/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.queries.spans.SpanOrQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Iterator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class SpanOrQueryBuilderTests extends AbstractQueryTestCase<SpanOrQueryBuilder> {
    @Override
    protected SpanOrQueryBuilder doCreateTestQueryBuilder() {
        SpanTermQueryBuilder[] spanTermQueries = new SpanTermQueryBuilderTests().createSpanTermQueryBuilders(randomIntBetween(1, 6));
        SpanOrQueryBuilder queryBuilder = new SpanOrQueryBuilder(spanTermQueries[0]);
        for (int i = 1; i < spanTermQueries.length; i++) {
            queryBuilder.addClause(spanTermQueries[i]);
        }
        return queryBuilder;
    }

    @Override
    protected SpanOrQueryBuilder createQueryWithInnerQuery(QueryBuilder queryBuilder) {
        if (queryBuilder instanceof SpanOrQueryBuilder) {
            return new SpanOrQueryBuilder((SpanOrQueryBuilder) queryBuilder);
        }
        return new SpanOrQueryBuilder(new SpanTermQueryBuilder("field", "value"));
    }

    @Override
    protected void doAssertLuceneQuery(SpanOrQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(query, instanceOf(SpanOrQuery.class));
        SpanOrQuery spanOrQuery = (SpanOrQuery) query;
        assertThat(spanOrQuery.getClauses().length, equalTo(queryBuilder.clauses().size()));
        Iterator<SpanQueryBuilder> spanQueryBuilderIterator = queryBuilder.clauses().iterator();
        for (SpanQuery spanQuery : spanOrQuery.getClauses()) {
            assertThat(spanQuery, equalTo(spanQueryBuilderIterator.next().toQuery(context)));
        }
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SpanOrQueryBuilder((SpanQueryBuilder) null));
        assertEquals("[span_or] must include at least one clause", e.getMessage());

        SpanOrQueryBuilder spanOrBuilder = new SpanOrQueryBuilder(new SpanTermQueryBuilder("field", "value"));
        e = expectThrows(IllegalArgumentException.class, () -> spanOrBuilder.addClause(null));
        assertEquals("[span_or] inner clause cannot be null", e.getMessage());
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
              "span_or" : {
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
                "boost" : 2.0
              }
            }""";

        SpanOrQueryBuilder parsed = (SpanOrQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 3, parsed.clauses().size());
        assertEquals(json, 2.0, parsed.boost(), 0.0);
    }

    public void testFromJsonWithNonDefaultBoostInInnerQuery() {
        String json = """
            {
              "span_or" : {
                "clauses" : [ {
                  "span_term" : {
                    "field" : {
                      "value" : "value1",
                      "boost" : 2.0
                    }
                  }
                } ],
                "boost" : 1.0
              }
            }""";

        Exception exception = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(exception.getMessage(), equalTo("span_or [clauses] as a nested span clause can't have non-default boost value [2.0]"));
    }

    public void testClausesBreakerEstimate() throws IOException {
        // BASELINE + clauses.size()*8. SpanTerm children are charged via namedObject before the parent.
        // SpanTermQueryBuilder("field","value"): 256 + (5*2+64) + (5+64) = 399.
        // limit = 2*childCost + BASELINE = 1054.
        // Without the override the parent charges exactly 256 (= limit, strict > → no trip).
        // With the override the parent charges 256 + 2*8 = 272 → total 1070 > 1054 → trips on
        // the collection overhead term.
        // small = 1 clause: 399 (child) + 264 (own) = 663 ≤ 1054 → passes.
        SpanTermQueryBuilder term = new SpanTermQueryBuilder("field", "value");
        long childCost = AbstractQueryBuilder.QUERY_BUILDER_SIZE_ESTIMATE_BYTES + ("field".length() * 2L + 64L) + ("value".length() + 64L);
        long limit = 2 * childCost + AbstractQueryBuilder.QUERY_BUILDER_SIZE_ESTIMATE_BYTES;
        LimitedBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(limit));
        AbstractQueryBuilder.setQueryParsingBreaker(breaker);
        try {
            SpanOrQueryBuilder small = new SpanOrQueryBuilder(term);
            SpanOrQueryBuilder big = new SpanOrQueryBuilder(term).addClause(term);
            for (XContentType type : new XContentType[] { XContentType.JSON, XContentType.SMILE }) {
                BytesReference bytes = XContentHelper.toXContent(small, type, false);
                try (XContentParser parser = createParser(type.xContent(), bytes)) {
                    parseQuery(parser);
                }
                BytesReference bigBytes = XContentHelper.toXContent(big, type, false);
                try (XContentParser parser = createParser(type.xContent(), bigBytes)) {
                    expectThrows(CircuitBreakingException.class, () -> parseQuery(parser));
                }
            }
        } finally {
            AbstractQueryBuilder.setQueryParsingBreaker(null);
        }
    }
}
