/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.support;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

/**
 * Keeps track of queries to be used as prefilters.
 * During {@link QueryBuilder#toQuery(SearchExecutionContext)}, each query pushes queries to be used
 * as prefilters to the {@link AutoPrefilteringScope}. Queries that need to apply prefilters can
 * fetch them by calling {@link #getPrefilters()}.
 *
 * The scope is implemented as a stack {@link Deque} of lists of prefilters.
 * As we move down the query tree, each query may push a list of prefilters.
 * A query that consumes prefilters fetches a flattened list of all prefilters in scope via {@link #getPrefilters()}.
 * When the query leaves the scope, {@link #pop()} should be called to remove the latest list of prefilters from the stack.
 * This way queries in other query tree branches will not fetch irrelevant prefilters.
 */
public final class AutoPrefilteringScope implements Releasable {

    private final Deque<List<QueryBuilder>> prefiltersStack = new LinkedList<>();

    /**
     * Pushes a list of prefilters to the scope.
     */
    public void push(List<QueryBuilder> prefilters) {
        prefiltersStack.push(prefilters);
    }

    /**
     * Removes the latest list of prefilters from the scope.
     */
    public void pop() {
        prefiltersStack.pop();
    }

    /**
     * Returns all prefilters in scope.
     */
    public List<QueryBuilder> getPrefilters() {
        return prefiltersStack.stream().flatMap(List::stream).toList();
    }

    @Override
    public void close() {
        pop();
    }
}
