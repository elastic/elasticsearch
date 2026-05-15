/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;

public class NestedFieldFilterQueryBuilderTests extends AbstractQueryTestCase<NestedFieldFilterQueryBuilder> {
    @Override
    protected NestedFieldFilterQueryBuilder doCreateTestQueryBuilder() {
        String filterFieldName = randomBoolean() ? KEYWORD_FIELD_NAME : TEXT_FIELD_NAME;
        return new NestedFieldFilterQueryBuilder(QueryBuilders.termQuery(filterFieldName, randomAlphaOfLength(10)));
    }

    @Override
    protected void doAssertLuceneQuery(NestedFieldFilterQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        // The fallback path produces a BooleanQuery with SHOULD clauses (filter + joined filter)
        assertThat(query, instanceOf(BooleanQuery.class));
    }

    @Override
    public void testUnknownField() throws IOException {
        // Test isn't relevant, since query is never parsed from xContent
    }

    @Override
    public void testUnknownObjectException() {
        // Test isn't relevant, since query is never parsed from xContent
    }

    @Override
    public void testFromXContent() throws IOException {
        // Test isn't relevant, since query is never parsed from xContent
    }

    @Override
    public void testValidOutput() {
        // Test isn't relevant, since query is never parsed from xContent
    }
}
