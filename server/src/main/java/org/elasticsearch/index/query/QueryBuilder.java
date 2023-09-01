/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.xcontent.ToXContentObject;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface QueryBuilder extends VersionedNamedWriteable, ToXContentObject, Rewriteable<QueryBuilder> {

    /**
     * Converts this QueryBuilder to a lucene {@link Query}.
     * Returns {@code null} if this query should be ignored in the context of
     * parent queries.
     *
     * @param context additional information needed to construct the queries
     * @return the {@link Query} or {@code null} if this query should be ignored upstream
     */
    Query toQuery(SearchExecutionContext context) throws IOException;

    /**
     * Sets the arbitrary name to be assigned to the query (see named queries).
     * Implementers should return the concrete type of the
     * {@link QueryBuilder} so that calls can be chained. This is done
     * automatically when extending {@link AbstractQueryBuilder}.
     */
    QueryBuilder queryName(String queryName);

    /**
     * Returns the arbitrary name assigned to the query (see named queries).
     */
    String queryName();

    /**
     * Returns the boost for this query.
     */
    float boost();

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     * Implementers should return the concrete type of the
     * {@link QueryBuilder} so that calls can be chained. This is done
     * automatically when extending {@link AbstractQueryBuilder}.
     */
    QueryBuilder boost(float boost);

    /**
     * Returns the name that identifies uniquely the query
     */
    String getName();

    /**
     * Rewrites this query builder into its primitive form. By default this method return the builder itself. If the builder
     * did not change the identity reference must be returned otherwise the builder will be rewritten infinitely.
     */
    @Override
    default QueryBuilder rewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        return this;
    }

    /**
     * Walks the query tree calling the consumer once per node.
     * Defaults to calling the query builder as a leaf in the tree.
     * Override to correctly walk the children of non-leaves.
     */
    default void visit(Visitor<?> visitor) {
        visitor.enter(this);
        visitor.exit(this);
    }

    /**
     * Callback mechanism for visiting each query builder in a query builder tree.
     */
    class Visitor<V extends Visitor<V>> {

        Map<Class<? extends QueryBuilder>, BiConsumer<QueryBuilder, V>> enterConsumers;
        Map<Class<? extends QueryBuilder>, BiConsumer<QueryBuilder, V>> exitConsumers;

        /**
         * Visits an individual query builder from top-down.
         */
        @SuppressWarnings("unchecked")
        public void enter(QueryBuilder queryBuilder) {
            BiConsumer<QueryBuilder, V> callback = enterConsumers.get(queryBuilder.getClass());
            if (callback != null) {
                callback.accept(queryBuilder, (V)this);
            }
        }

        /**
         * Visits an individual query builder bottom-up.
         */
        @SuppressWarnings("unchecked")
        public void exit(QueryBuilder queryBuilder) {
            BiConsumer<QueryBuilder, V> callback = exitConsumers.get(queryBuilder.getClass());
            if (callback != null) {
                callback.accept(queryBuilder, (V)this);
            }
        }
    }
}
