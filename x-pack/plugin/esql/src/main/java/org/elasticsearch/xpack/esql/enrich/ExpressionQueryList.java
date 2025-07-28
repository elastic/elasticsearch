/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.compute.operator.lookup.LookupEnrichQueryGenerator;
import org.elasticsearch.compute.operator.lookup.QueryList;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;

import java.io.IOException;
import java.util.List;

/**
 * A {@link LookupEnrichQueryGenerator} that combines multiple {@link QueryList}s into a single query.
 * Each query in the resulting query will be a conjunction of all queries from the input lists at the same position.
 * In the future we can extend this to support more complex expressions, such as disjunctions or negations.
 */
public class ExpressionQueryList implements LookupEnrichQueryGenerator {
    private final List<QueryList> queryLists;
    private final QueryBuilder preJoinFilter;
    private final SearchExecutionContext context;

    public ExpressionQueryList(List<QueryList> queryLists, SearchExecutionContext context, QueryBuilder preJoinFilter) {
        if (queryLists.size() < 2 && Literal.TRUE.equals(preJoinFilter)) {
            throw new IllegalArgumentException("ExpressionQueryList must have at least two QueryLists");
        }
        this.queryLists = queryLists;
        this.preJoinFilter = preJoinFilter;
        this.context = context;
    }

    @Override
    public Query getQuery(int position) throws IOException {
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
        // also attach the pre-join filter if it exists
        /*if (Literal.TRUE.equals(preJoinFilter) == false) {
            if (preJoinFilter instanceof TranslationAware translationAware) {
                Query preJoinQuery = tryToGetAsLuceneQuery(translationAware);
                if (preJoinQuery == null) {
                    preJoinQuery = tryToGetThroughQueryBuilder(translationAware);
                }
                if (preJoinQuery == null) {
                    throw new UnsupportedOperationException("Cannot translate pre-join filter to Lucene query: " + preJoinFilter);
                }
                builder.add(preJoinQuery, BooleanClause.Occur.FILTER);
            }
        }*/
        if (preJoinFilter != null) {
            // JULIAN TO DO: Can we precompile the query? I don't want to call toQuery for every row
            builder.add(preJoinFilter.toQuery(context), BooleanClause.Occur.FILTER);
        }
        return builder.build();
    }

    /*private Query tryToGetThroughQueryBuilder(TranslationAware translationAware) {
        // it seems I might need to pass a QueryBuilder, instead of Expression directly????
        // can a QueryBuilder support nested complex expressions with AND, OR, NOT?
        return translationAware.asQuery(WHAT_GOES_HERE, WHAT_GOES_HERE).toQueryBuilder().toQuery(queryLists.get(0).searchExecutionContext);
    }

    private Query tryToGetAsLuceneQuery(TranslationAware translationAware) {
        // attempt to translate directly to a Lucene Query
        // not sure how to get the field name from the expression
        MappedFieldType fieldType = context.getFieldType(WHAT_GOES_HERE.fieldName().string());
        try {
            return translationAware.asLuceneQuery(fieldType, CONSTANT_SCORE_REWRITE, context);
        } catch (Exception e) {}
        // only a few expression types support asLuceneQuery, it is OK to fail here and we will try a different approach
        return null;
    }
    */
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
        return positionCount;
    }
}
