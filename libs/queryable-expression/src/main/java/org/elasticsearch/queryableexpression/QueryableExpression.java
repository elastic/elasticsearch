/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.queryableexpression;

import org.apache.lucene.search.MatchAllDocsQuery;

/**
 * An expression that can be approximated with queries. See
 * {@link #castToLong()} for a way to make queries.
 */
public interface QueryableExpression {
    /**
     * An expression that can not be queried, so it always returned
     * {@link MatchAllDocsQuery}.
     */
    QueryableExpression UNQUERYABLE = UnqueryableExpression.UNQUERYABLE;

    QueryableExpression add(QueryableExpression rhs);

    QueryableExpression multiply(QueryableExpression rhs);

    QueryableExpression divide(QueryableExpression rhs);

    LongQueryableExpression castToLong();
}
