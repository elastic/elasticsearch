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
        QueryBuilder namedQuery = interceptedQueryBuilderWrapper.queryName(randomAlphaOfLengthBetween(5, 10));
        assertTrue(namedQuery instanceof InterceptedQueryBuilderWrapper);
    }

    public void testQueryBoostReturnsWrappedQueryBuilder() {
        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
        InterceptedQueryBuilderWrapper interceptedQueryBuilderWrapper = new InterceptedQueryBuilderWrapper(matchAllQueryBuilder);
        QueryBuilder boostedQuery = interceptedQueryBuilderWrapper.boost(randomFloat());
        assertTrue(boostedQuery instanceof InterceptedQueryBuilderWrapper);
    }
}
