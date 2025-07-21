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

public class ExpressionQueryList implements LookupEnrichQueryGenerator {
    List<QueryList> queryLists;

    public ExpressionQueryList(List<QueryList> queryLists) {
        if (queryLists.size() < 2) {
            throw new IllegalArgumentException("ExpressionQueryList must have at least two QueryLists");
        }
        this.queryLists = queryLists;
    }

    @Override
    public Query getQuery(int position) {
        // JULIAN: Do we have a guarantee that the positions will match across all QueryLists?
        // for now we only support AND of the queries in the lists
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (QueryList queryList : queryLists) {
            Query q = queryList.getQuery(position);
            builder.add(q, BooleanClause.Occur.MUST);

        }
        return builder.build();
    }

    @Override
    public int getPositionCount() {
        int positionCount = queryLists.get(0).getPositionCount();
        for (QueryList queryList : queryLists) {
            if (queryList.getPositionCount() != positionCount) {
                throw new IllegalArgumentException(
                    "All QueryLists must have the same position count, expected: "
                        + positionCount
                        + ", but got: "
                        + queryList.getPositionCount()
                );
            }
        }
        return queryLists.get(0).getPositionCount();
    }
}
