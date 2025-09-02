/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import java.util.List;

/**
 * A {@link LookupEnrichQueryGenerator} that combines multiple {@link QueryList}s into a single query.
 * Each query in the resulting query will be a conjunction of all queries from the input lists at the same position.
 * In the future we can extend this to support more complex expressions, such as disjunctions or negations.
 */
public class ExpressionQueryList implements LookupEnrichQueryGenerator {
    private final List<QueryList> queryLists;

    public ExpressionQueryList(List<QueryList> queryLists) {
        if (queryLists.size() < 2) {
            throw new IllegalArgumentException("ExpressionQueryList must have at least two QueryLists");
        }
        this.queryLists = queryLists;
    }

    @Override
    public Query getQuery(int position) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (QueryList queryList : queryLists) {
            Query q = queryList.getQuery(position);
            if (q == null) {
                // if any of the matchFields are null, it means there is no match for this position
                // A AND NULL is always NULL, so we can skip this position
                return null;
            }
            builder.add(q, BooleanClause.Occur.FILTER);
        }
        return builder.build();
    }

    @Override
    public int getPositionCount() {
        int positionCount = queryLists.get(0).getPositionCount();
        for (QueryList queryList : queryLists) {
            if (queryList.getPositionCount() != positionCount) {
                throw new IllegalStateException(
                    "All QueryLists must have the same position count, expected: "
                        + positionCount
                        + ", but got: "
                        + queryList.getPositionCount()
                );
            }
        }
        return positionCount;
    }
}
