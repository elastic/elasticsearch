/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.queries;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class TextExpansionQueryBuilderTests extends AbstractQueryTestCase<TextExpansionQueryBuilder> {

    // /TODO implement createQueryWithInnerQuery() and testMaxNestedDepth??

    @Override
    protected TextExpansionQueryBuilder doCreateTestQueryBuilder() {
        var builder = new TextExpansionQueryBuilder(
            randomAlphaOfLength(4),
            randomAlphaOfLength(4),
            randomBoolean() ? null : randomAlphaOfLength(4)
        );
        if (randomBoolean()) {
            builder.boost((float) randomDoubleBetween(0.1, 10.0, true));
        }
        if (randomBoolean()) {
            builder.queryName(randomAlphaOfLength(4));
        }
        return builder;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(MachineLearning.class);
    }

    @Override
    protected void doAssertLuceneQuery(TextExpansionQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        fail();
    }

    public void testIllegalValues() {
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new TextExpansionQueryBuilder(null, "model text", null)
            );
            assertEquals("[text_expansion] requires a fieldName", e.getMessage());
        }
        {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new TextExpansionQueryBuilder("field name", null, null)
            );
            assertEquals("[text_expansion] requires a model_text value", e.getMessage());
        }
    }

    public void testToXContentWithDefaults() throws IOException {
        QueryBuilder query = new TextExpansionQueryBuilder("foo", "bar", null);
        checkGeneratedJson("""
            {
              "text_expansion": {
                "foo": {
                  "model_text": "bar",
                  "model_id": "slim"
                }
              }
            }""", query);
    }
}
