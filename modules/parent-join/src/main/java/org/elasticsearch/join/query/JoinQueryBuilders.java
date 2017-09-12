/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
