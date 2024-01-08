/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;

import java.io.IOException;

/**
 * This is an extension of {@link DiversifyingChildrenFloatKnnVectorQuery} that gathers the top matching children,
 * not just the single nearest. This is useful for gathering inner hits.
 */
public class ESDiversifyingChildrenFloatKnnVectorQuery extends DiversifyingChildrenFloatKnnVectorQuery {

    private final boolean scoreAllMatchingChildren;
    private final BitSetProducer parentsFilter;
    private final float[] query;

    /**
     * @param field         the query field
     * @param query         the vector query
     * @param childFilter   the child filter
     * @param k             how many parent documents to return given the matching children
     * @param parentsFilter identifying the parent documents.
     * @param scoreAllMatchingChildren whether to score all matching children or just the single nearest
     */
    public ESDiversifyingChildrenFloatKnnVectorQuery(
        String field,
        float[] query,
        Query childFilter,
        int k,
        BitSetProducer parentsFilter,
        boolean scoreAllMatchingChildren
    ) {
        super(field, query, childFilter, k, parentsFilter);
        this.scoreAllMatchingChildren = scoreAllMatchingChildren;
        this.parentsFilter = parentsFilter;
        this.query = query;
    }

    /**
     * This will first execute as a normal {@link DiversifyingChildrenFloatKnnVectorQuery}. If {@code matchAllChildren} is true
     * it will then provide a query that will score every child that matches the provided childFilter.
     * If {@code matchAllChildren} is false it will provide a query that will score only the single nearest child.
     */
    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        // This gathers the nearest single child for each parent
        Query rewritten = super.rewrite(indexSearcher);
        if (scoreAllMatchingChildren == false) {
            return rewritten;
        }
        return new ChildToParentToChildJoinVectorQuery(
            rewritten,
            ChildToParentToChildJoinVectorQuery.fromField(query, field),
            parentsFilter
        );
    }
}
