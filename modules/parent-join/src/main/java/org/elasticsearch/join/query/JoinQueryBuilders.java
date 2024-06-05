/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.join.query;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.QueryBuilder;

public abstract class JoinQueryBuilders {
    /**
     * Constructs a new has_child query, with the child type and the query to run on the child documents. The
     * results of this query are the parent docs that those child docs matched.
     *
     * @param type      The child type.
     * @param query     The query.
     * @param scoreMode How the scores from the children hits should be aggregated into the parent hit.
     */
    public static HasChildQueryBuilder hasChildQuery(String type, QueryBuilder query, ScoreMode scoreMode) {
        return new HasChildQueryBuilder(type, query, scoreMode);
    }

    /**
     * Constructs a new parent query, with the parent type and the query to run on the parent documents. The
     * results of this query are the children docs that those parent docs matched.
     *
     * @param type      The parent type.
     * @param query     The query.
     * @param score     Whether the score from the parent hit should propagate to the child hit
     */
    public static HasParentQueryBuilder hasParentQuery(String type, QueryBuilder query, boolean score) {
        return new HasParentQueryBuilder(type, query, score);
    }

    /**
     * Constructs a new parent id query that returns all child documents of the specified type that
     * point to the specified id.
     */
    public static ParentIdQueryBuilder parentId(String type, String id) {
        return new ParentIdQueryBuilder(type, id);
    }
}
