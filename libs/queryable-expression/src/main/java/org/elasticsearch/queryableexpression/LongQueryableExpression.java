/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.queryableexpression;

import org.apache.lucene.search.Query;

import java.util.function.LongFunction;

public interface LongQueryableExpression extends QueryableExpression {
    static LongQueryableExpression constant(long n) {
        return new AbstractLongQueryableExpression.Constant(n);
    }

    interface LongQueries {
        Query approximateTermQuery(long term);

        Query approximateRangeQuery(long lower, long upper);
    }

    static LongQueryableExpression field(String name, LongQueries queries) {
        return new AbstractLongQueryableExpression.Field(name, queries);
    }

    interface IntQueries {
        Query approximateTermQuery(int term);

        Query approximateRangeQuery(int lower, int upper);
    }

    static LongQueryableExpression field(String name, IntQueries queries) {
        return field(name, new AbstractLongQueryableExpression.IntQueriesToLongQueries(queries));
    }

    Query approximateTermQuery(long term);

    Query approximateRangeQuery(long lower, long upper);

    QueryableExpression mapConstant(LongFunction<QueryableExpression> map);
}
