/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.ReanalyzingTextQuery;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class QueriesTests extends ESTestCase {

    public void testNonNestedQuery() {
        // This is a custom query that extends AutomatonQuery and want to make sure the equals method works
        assertEquals(Queries.newNonNestedFilter(IndexVersion.current()), Queries.newNonNestedFilter(IndexVersion.current()));
        assertEquals(
            Queries.newNonNestedFilter(IndexVersion.current()).hashCode(),
            Queries.newNonNestedFilter(IndexVersion.current()).hashCode()
        );
        assertEquals(Queries.newNonNestedFilter(IndexVersion.current()), new FieldExistsQuery(SeqNoFieldMapper.PRIMARY_TERM_NAME));
    }

    public void testIsNegativeQuery() {
        assertFalse(Queries.isNegativeQuery(Queries.ALL_DOCS_INSTANCE));
        assertFalse(Queries.isNegativeQuery(new BooleanQuery.Builder().build()));
        assertFalse(Queries.isNegativeQuery(new BooleanQuery.Builder().add(new TermQuery(new Term("foo", "bar")), Occur.MUST).build()));
        assertTrue(Queries.isNegativeQuery(new BooleanQuery.Builder().add(new TermQuery(new Term("foo", "bar")), Occur.MUST_NOT).build()));
        assertFalse(
            Queries.isNegativeQuery(
                new BooleanQuery.Builder().add(Queries.ALL_DOCS_INSTANCE, Occur.MUST).add(Queries.ALL_DOCS_INSTANCE, Occur.MUST_NOT).build()
            )
        );
    }

    public void testMinimumShouldMatchSeesThroughReanalyzingTextQuery() {
        final BooleanQuery bool = new BooleanQuery.Builder().add(new TermQuery(new Term("foo", "a")), Occur.SHOULD)
            .add(new TermQuery(new Term("foo", "b")), Occur.SHOULD)
            .build();
        final boolean scansEveryDocument = randomBoolean();
        final ReanalyzingTextQuery wrapped = new ReanalyzingTextQuery(
            bool,
            ctx -> doc -> List.of(),
            new StandardAnalyzer(),
            scansEveryDocument
        );
        final Query expected = new ReanalyzingTextQuery(
            Queries.applyMinimumShouldMatch(bool, "2"),
            ctx -> doc -> List.of(),
            new StandardAnalyzer(),
            scansEveryDocument
        );

        assertEquals(expected, Queries.maybeApplyMinimumShouldMatch(wrapped, "2"));
        assertEquals(new ConstantScoreQuery(expected), Queries.maybeApplyMinimumShouldMatch(new ConstantScoreQuery(wrapped), "2"));

        // Nothing to apply leaves the query as it is.
        assertSame(wrapped, Queries.maybeApplyMinimumShouldMatch(wrapped, null));
        assertSame(wrapped, Queries.maybeApplyMinimumShouldMatch(wrapped, "0"));
        final ReanalyzingTextQuery term = wrapped.withQuery(new TermQuery(new Term("foo", "a")));
        assertSame(term, Queries.maybeApplyMinimumShouldMatch(term, "2"));
        // A constant score query around anything but the wrapper is left alone, as before.
        final Query constantBool = new ConstantScoreQuery(bool);
        assertSame(constantBool, Queries.maybeApplyMinimumShouldMatch(constantBool, "2"));
    }

    public void testFixNegativeQuery() {
        assertEquals(
            new BooleanQuery.Builder().add(Queries.ALL_DOCS_INSTANCE, Occur.FILTER)
                .add(new TermQuery(new Term("foo", "bar")), Occur.MUST_NOT)
                .build(),
            Queries.fixNegativeQueryIfNeeded(
                new BooleanQuery.Builder().add(new TermQuery(new Term("foo", "bar")), Occur.MUST_NOT).build(),
                QueryVisitor.EMPTY_VISITOR
            )
        );
    }
}
