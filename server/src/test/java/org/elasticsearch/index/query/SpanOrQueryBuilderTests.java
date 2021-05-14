/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.test.AbstractQueryTestCase;

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
        expectThrows(UnsupportedOperationException.class,
                () -> spanNearQueryBuilder.clauses().add(new SpanTermQueryBuilder("field", "value2")));
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"span_or\" : {\n" +
                "    \"clauses\" : [ {\n" +
                "      \"span_term\" : {\n" +
                "        \"field\" : {\n" +
                "          \"value\" : \"value1\",\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    }, {\n" +
                "      \"span_term\" : {\n" +
                "        \"field\" : {\n" +
                "          \"value\" : \"value2\",\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    }, {\n" +
                "      \"span_term\" : {\n" +
                "        \"field\" : {\n" +
                "          \"value\" : \"value3\",\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    } ],\n" +
                "    \"boost\" : 2.0\n" +
                "  }\n" +
                "}";

        SpanOrQueryBuilder parsed = (SpanOrQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, 3, parsed.clauses().size());
        assertEquals(json, 2.0, parsed.boost(), 0.0);
    }

    public void testFromJsonWithNonDefaultBoostInInnerQuery() {
        String json =
                "{\n" +
                "  \"span_or\" : {\n" +
                "    \"clauses\" : [ {\n" +
                "      \"span_term\" : {\n" +
                "        \"field\" : {\n" +
                "          \"value\" : \"value1\",\n" +
                "          \"boost\" : 2.0\n" +
                "        }\n" +
                "      }\n" +
                "    } ],\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        Exception exception = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(exception.getMessage(),
            equalTo("span_or [clauses] as a nested span clause can't have non-default boost value [2.0]"));
    }
}
