/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.capabilities;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;

/**
 * Defines objects that need to go through the rewrite phase.
 */
public interface RewriteableAware extends TranslationAware {

    /**
     * @return The current active query builder.
     */
    QueryBuilder queryBuilder();

    /**
     * Replaces the current query builder with a rewritten iteration. This happens multiple times through the rewrite phase until
     * the final iteration of the query builder is stored.
     * @param queryBuilder QueryBuilder
     * @return Expression defining the active QueryBuilder
     */
    Expression replaceQueryBuilder(QueryBuilder queryBuilder);

}
