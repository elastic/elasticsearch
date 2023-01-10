/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.queries;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class TextEmbeddingQueryBuilderTests extends AbstractQueryTestCase<TextEmbeddingQueryBuilder> {
    @Override
    protected TextEmbeddingQueryBuilder doCreateTestQueryBuilder() {
        var builder = new TextEmbeddingQueryBuilder(
            randomAlphaOfLength(4),
            randomAlphaOfLength(4),
            randomAlphaOfLength(4),
            randomIntBetween(10, 20),
            randomIntBetween(50, 100)
        );
        if (randomBoolean()) {
            builder.boost((float) randomDoubleBetween(0.1, 10.0, true));
        }
        return builder;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(MachineLearning.class);
    }

    @Override
    protected void doAssertLuceneQuery(TextEmbeddingQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {

    }
}
