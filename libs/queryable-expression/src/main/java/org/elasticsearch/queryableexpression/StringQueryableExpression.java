/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.queryableexpression;

import org.apache.lucene.search.Query;

/**
 * {@code string} flavored expression.
 */
public interface StringQueryableExpression extends QueryableExpression {
    StringQueryableExpression substring();

    /**
     * Build a query that approximates a term query on this expression.
     */
    Query approximateTermQuery(String term);

    /**
     * Build a query that approximates query for a substring of a term.
     */
    Query approximateSubstringQuery(String term);

    interface Queries {
        Query approximateExists();

        Query approximateTermQuery(String term);

        Query approximateSubstringQuery(String term);
    }

    static StringQueryableExpression field(String name, Queries queries) {
        return new AbstractStringQueryableExpression.Field(name, queries);
    }

}
