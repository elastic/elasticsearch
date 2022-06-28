/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.Version;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

public class QueriesTests extends ESTestCase {

    public void testNonNestedQuery() {
        for (Version version : VersionUtils.allVersions()) {
            // This is a custom query that extends AutomatonQuery and want to make sure the equals method works
            assertEquals(Queries.newNonNestedFilter(version), Queries.newNonNestedFilter(version));
            assertEquals(Queries.newNonNestedFilter(version).hashCode(), Queries.newNonNestedFilter(version).hashCode());
            if (version.onOrAfter(Version.V_6_1_0)) {
                assertEquals(Queries.newNonNestedFilter(version), new DocValuesFieldExistsQuery(SeqNoFieldMapper.PRIMARY_TERM_NAME));
            } else {
                assertEquals(
                    Queries.newNonNestedFilter(version),
                    new BooleanQuery.Builder().add(new MatchAllDocsQuery(), BooleanClause.Occur.FILTER)
                        .add(Queries.newNestedFilter(), BooleanClause.Occur.MUST_NOT)
                        .build()
                );
            }
        }
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
