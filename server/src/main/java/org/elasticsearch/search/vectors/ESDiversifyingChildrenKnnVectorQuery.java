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
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.DiversifyingChildrenByteKnnVectorQuery;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;

import java.io.IOException;
import java.util.Objects;

/**
 * This is an extension of {@link DiversifyingChildrenByteKnnVectorQuery} that gathers the top matching children,
 * not just the single nearest. This is useful for gathering inner hits.
 */
public class ESDiversifyingChildrenKnnVectorQuery extends Query {

    private final Query nestedKnnQuery, exactKnnQuery;
    private final BitSetProducer parentsFilter;

    /**
     * @param nestedKnnQuery the nested kNN query
     * @param parentsFilter  identifying the parent documents.
     * @param exactKnnQuery  the exact kNN query
     */
    public ESDiversifyingChildrenKnnVectorQuery(Query nestedKnnQuery, BitSetProducer parentsFilter, Query exactKnnQuery) {
        this.nestedKnnQuery = nestedKnnQuery;
        this.exactKnnQuery = exactKnnQuery;
        this.parentsFilter = parentsFilter;
    }

    @Override
    public String toString(String field) {
        return "ESDiversifyingChildrenKnnVectorQuery";
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
        return new ChildToParentToChildJoinVectorQuery(rewritten, exactKnnQuery, parentsFilter);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        throw new IllegalArgumentException("rewrite() must be called first");
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        ESDiversifyingChildrenKnnVectorQuery other = (ESDiversifyingChildrenKnnVectorQuery) obj;
        return Objects.equals(nestedKnnQuery, other.nestedKnnQuery)
            && Objects.equals(exactKnnQuery, other.exactKnnQuery)
            && Objects.equals(parentsFilter, other.parentsFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nestedKnnQuery, exactKnnQuery, parentsFilter);
    }

}
