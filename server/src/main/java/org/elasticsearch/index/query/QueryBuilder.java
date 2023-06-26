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
     * Converts this QueryBuilder to a lucene {@link Query}.
     * Returns {@code null} if this query should be ignored in the context of highlighting.
     *
     * @param fieldName name of the field we want to highlight
     * @return the {@link Query} or {@code null} if this query should have standard behaviour
     * for highlighting.
     */
    default Query toHighlightQuery(String fieldName) {
        return null;
    }

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
}
