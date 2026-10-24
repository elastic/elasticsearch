/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.search.NestedHelper;
import org.junit.Before;

import java.io.IOException;

/**
 * Tests that {@link NestedLevelJoinQueryBuilder} joins its filter to the nested level of the vector field, whatever the nested scope.
 */
public class NestedLevelJoinQueryBuilderTests extends MapperServiceTestCase {

    private SearchExecutionContext context;

    @Before
    public void initContext() throws IOException {
        context = createSearchExecutionContext(createMapperService("""
            { "_doc": { "properties": {
              "genre": { "type": "keyword" },
              "vector": { "type": "dense_vector", "dims": 2, "similarity": "l2_norm" },
              "chapters": { "type": "nested", "properties": {
                "vector": { "type": "dense_vector", "dims": 2, "similarity": "l2_norm" }
              } }
            } } }
            """));
    }

    public void testJoinsToTheVectorLevel() throws IOException {
        TermQueryBuilder filter = new TermQueryBuilder("genre", "novel");
        Query term = filter.toQuery(context);

        context.nestedScope().nextLevel(context.nestedLookup().getNestedMappers().get("chapters"));
        try {
            assertEquals(term, new NestedLevelJoinQueryBuilder(filter, "vector").toQuery(context));
            assertEquals(
                NestedHelper.joinToNestedLevel(term, "chapters", null, context),
                new NestedLevelJoinQueryBuilder(filter, "chapters.vector").toQuery(context)
            );
        } finally {
            context.nestedScope().previousLevel();
        }
    }

    public void testTransient() {
        NestedLevelJoinQueryBuilder join = new NestedLevelJoinQueryBuilder(new TermQueryBuilder("genre", "novel"), "chapters.vector");
        expectThrows(IllegalStateException.class, () -> join.writeTo(new BytesStreamOutput()));
        expectThrows(AssertionError.class, () -> join.rewrite(context));
    }
}
