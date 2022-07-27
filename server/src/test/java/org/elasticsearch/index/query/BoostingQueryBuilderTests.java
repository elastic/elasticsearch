/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;

public class BoostingQueryBuilderTests extends AbstractQueryTestCase<BoostingQueryBuilder> {

    @Override
    protected BoostingQueryBuilder doCreateTestQueryBuilder() {
        BoostingQueryBuilder query = new BoostingQueryBuilder(
            RandomQueryBuilder.createQuery(random()),
            RandomQueryBuilder.createQuery(random())
        );
        query.negativeBoost(2.0f / randomIntBetween(1, 20));
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(BoostingQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        Query positive = queryBuilder.positiveQuery().rewrite(context).toQuery(context);
        Query negative = queryBuilder.negativeQuery().rewrite(context).toQuery(context);
        if (positive == null || negative == null) {
            assertThat(query, nullValue());
        } else if (positive instanceof MatchNoDocsQuery) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        } else {
            assertThat(query, instanceOf(FunctionScoreQuery.class));
        }
    }

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () -> new BoostingQueryBuilder(null, new MatchAllQueryBuilder()));
        expectThrows(IllegalArgumentException.class, () -> new BoostingQueryBuilder(new MatchAllQueryBuilder(), null));
        expectThrows(
            IllegalArgumentException.class,
            () -> new BoostingQueryBuilder(new MatchAllQueryBuilder(), new MatchAllQueryBuilder()).negativeBoost(-1.0f)
        );
    }

    public void testFromJson() throws IOException {
        String query = """
            {
              "boosting" : {
                "positive" : {
                  "term" : {
                    "field1" : {
                      "value" : "value1",
                      "boost" : 5.0
                    }
                  }
                },
                "negative" : {
                  "term" : {
                    "field2" : {
                      "value" : "value2",
                      "boost" : 8.0
                    }
                  }
                },
                "negative_boost" : 23.0,
                "boost" : 42.0
              }
            }""";

        BoostingQueryBuilder queryBuilder = (BoostingQueryBuilder) parseQuery(query);
        checkGeneratedJson(query, queryBuilder);
        assertEquals(query, 42, queryBuilder.boost(), 0.00001);
        assertEquals(query, 23, queryBuilder.negativeBoost(), 0.00001);
        assertEquals(query, 8, queryBuilder.negativeQuery().boost(), 0.00001);
        assertEquals(query, 5, queryBuilder.positiveQuery().boost(), 0.00001);
    }

    public void testRewrite() throws IOException {
        QueryBuilder positive = randomBoolean()
            ? new MatchAllQueryBuilder()
            : new WrapperQueryBuilder(new TermQueryBuilder(KEYWORD_FIELD_NAME, "bar").toString());
        QueryBuilder negative = randomBoolean()
            ? new MatchAllQueryBuilder()
            : new WrapperQueryBuilder(new TermQueryBuilder(TEXT_FIELD_NAME, "bar").toString());
        BoostingQueryBuilder qb = new BoostingQueryBuilder(positive, negative);
        QueryBuilder rewrite = qb.rewrite(createSearchExecutionContext());
        if (positive instanceof MatchAllQueryBuilder && negative instanceof MatchAllQueryBuilder) {
            assertSame(rewrite, qb);
        } else {
            assertNotSame(rewrite, qb);
            assertEquals(
                new BoostingQueryBuilder(
                    positive.rewrite(createSearchExecutionContext()),
                    negative.rewrite(createSearchExecutionContext())
                ),
                rewrite
            );
        }
    }

    public void testRewriteToMatchNone() throws IOException {
        BoostingQueryBuilder builder = new BoostingQueryBuilder(
            new TermQueryBuilder("unmapped_field", "value"),
            new TermQueryBuilder(KEYWORD_FIELD_NAME, "other_value")
        );
        QueryBuilder rewrite = builder.rewrite(createSearchExecutionContext());
        assertThat(rewrite, instanceOf(MatchNoneQueryBuilder.class));
    }

    @Override
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);

        BoostingQueryBuilder queryBuilder1 = new BoostingQueryBuilder(
            new TermQueryBuilder("unmapped_field", "foo"),
            new MatchNoneQueryBuilder()
        );
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> queryBuilder1.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());

        BoostingQueryBuilder queryBuilder2 = new BoostingQueryBuilder(
            new MatchAllQueryBuilder(),
            new TermQueryBuilder("unmapped_field", "foo")
        );
        e = expectThrows(IllegalStateException.class, () -> queryBuilder2.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }
}
