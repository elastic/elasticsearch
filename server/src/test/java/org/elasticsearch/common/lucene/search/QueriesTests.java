/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.test.ESTestCase;

public class QueriesTests extends ESTestCase {

    public void testNonNestedQuery() {
        // This is a custom query that extends AutomatonQuery and want to make sure the equals method works
        assertEquals(Queries.newNonNestedFilter(), Queries.newNonNestedFilter());
        assertEquals(Queries.newNonNestedFilter().hashCode(), Queries.newNonNestedFilter().hashCode());
        assertEquals(Queries.newNonNestedFilter(), new FieldExistsQuery(SeqNoFieldMapper.PRIMARY_TERM_NAME));
    }

    public void testIsNegativeQuery() {
        assertFalse(Queries.isNegativeQuery(new MatchAllDocsQuery()));
        assertFalse(Queries.isNegativeQuery(new BooleanQuery.Builder().build()));
        assertFalse(Queries.isNegativeQuery(new BooleanQuery.Builder().add(new TermQuery(new Term("foo", "bar")), Occur.MUST).build()));
        assertTrue(Queries.isNegativeQuery(new BooleanQuery.Builder().add(new TermQuery(new Term("foo", "bar")), Occur.MUST_NOT).build()));
        assertFalse(
            Queries.isNegativeQuery(
                new BooleanQuery.Builder().add(new MatchAllDocsQuery(), Occur.MUST).add(new MatchAllDocsQuery(), Occur.MUST_NOT).build()
            )
        );
    }

    public void testFixNegativeQuery() {
        assertEquals(
            new BooleanQuery.Builder().add(new MatchAllDocsQuery(), Occur.FILTER)
                .add(new TermQuery(new Term("foo", "bar")), Occur.MUST_NOT)
                .build(),
            Queries.fixNegativeQueryIfNeeded(new BooleanQuery.Builder().add(new TermQuery(new Term("foo", "bar")), Occur.MUST_NOT).build())
        );
    }
}
