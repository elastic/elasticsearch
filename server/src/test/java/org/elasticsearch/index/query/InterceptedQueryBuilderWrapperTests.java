/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.test.ESTestCase;

public class InterceptedQueryBuilderWrapperTests extends ESTestCase {

    public void testQueryNameReturnsWrappedQueryBuilder() {
        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
        InterceptedQueryBuilderWrapper interceptedQueryBuilderWrapper = new InterceptedQueryBuilderWrapper(matchAllQueryBuilder);
        String queryName = randomAlphaOfLengthBetween(5, 10);
        QueryBuilder namedQuery = interceptedQueryBuilderWrapper.queryName(queryName);
        assertTrue(namedQuery instanceof InterceptedQueryBuilderWrapper);
        assertEquals(queryName, namedQuery.queryName());
    }

    public void testQueryBoostReturnsWrappedQueryBuilder() {
        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
        InterceptedQueryBuilderWrapper interceptedQueryBuilderWrapper = new InterceptedQueryBuilderWrapper(matchAllQueryBuilder);
        float boost = randomFloat();
        QueryBuilder boostedQuery = interceptedQueryBuilderWrapper.boost(boost);
        assertTrue(boostedQuery instanceof InterceptedQueryBuilderWrapper);
        assertEquals(boost, boostedQuery.boost(), 0.0001f);
    }
}
