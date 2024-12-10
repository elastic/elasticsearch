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

    public void testInterceptedQueryBuildersAreUnwrapped() {
        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();
        InterceptedQueryBuilderWrapper interceptedQueryBuilderWrapper = new InterceptedQueryBuilderWrapper(matchAllQueryBuilder);
        assertNotEquals(matchAllQueryBuilder, interceptedQueryBuilderWrapper);
        assertEquals(matchAllQueryBuilder, interceptedQueryBuilderWrapper.queryBuilder);
        InterceptedQueryBuilderWrapper interceptedQueryBuilderWrapper2 = new InterceptedQueryBuilderWrapper(interceptedQueryBuilderWrapper);
        assertNotEquals(matchAllQueryBuilder, interceptedQueryBuilderWrapper2);
        assertEquals(interceptedQueryBuilderWrapper, interceptedQueryBuilderWrapper2);
        assertEquals(matchAllQueryBuilder, interceptedQueryBuilderWrapper2.queryBuilder);
    }

}
