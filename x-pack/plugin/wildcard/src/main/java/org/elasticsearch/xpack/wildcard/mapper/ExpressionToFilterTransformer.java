/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.wildcard.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.wildcard.mapper.regex.Expression;

/**
 * Transforms expressions to filters.
 */
public class ExpressionToFilterTransformer implements Expression.Transformer<String, Query> {
    private final String ngramField;

    public ExpressionToFilterTransformer(String ngramField) {
        this.ngramField = ngramField;
    }

    @Override
    public Query alwaysTrue() {
        throw new IllegalArgumentException("Can't transform always true into a filter.");
    }

    @Override
    public Query alwaysFalse() {
        throw new IllegalArgumentException("Can't transform always false into a filter.");
    }

    @Override
    public Query leaf(String t) {
        return new TermQuery(new Term(ngramField, t));
    }

    @Override
    public Query and(Set<Query> js) {
        Builder and = new BooleanQuery.Builder();        
        for (Query j : js) {
            and.add(j, Occur.MUST);
        }
        return and.build();
    }

    @Override
    public Query or(Set<Query> js) {
        // Array containing all terms if this is contains only term filters
        boolean allTermFilters = true;
        List<BytesRef> allTerms = null;
        Builder filter = new BooleanQuery.Builder();        
        for (Query j : js) {
            filter.add(j, Occur.SHOULD);
            if (allTermFilters) {
                allTermFilters = j instanceof TermQuery;
                if (allTermFilters) {
                    if (allTerms == null) {
                        allTerms = new ArrayList<>(js.size());
                    }
                    allTerms.add(((TermQuery) j).getTerm().bytes());
                }
            }
        }
        if (!allTermFilters) {
            return filter.build();
        }
        return new TermInSetQuery(ngramField, allTerms);
    }
}
