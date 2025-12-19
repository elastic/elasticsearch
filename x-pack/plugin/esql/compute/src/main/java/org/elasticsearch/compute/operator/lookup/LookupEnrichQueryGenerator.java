/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.apache.lucene.search.Query;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.SearchExecutionContext;

/**
 * An interface to generates queries for the lookup and enrich operators.
 * This interface is used to retrieve queries based on a position index.
 * SearchExecutionContext is passed dynamically to avoid caching stale references.
 */
public interface LookupEnrichQueryGenerator {

    /**
     * Returns the query at the given position.
     * @param position The position index
     * @param inputPage The input page containing values
     * @param searchExecutionContext The search execution context (must be current, not cached)
     */
    @Nullable
    Query getQuery(int position, Page inputPage, SearchExecutionContext searchExecutionContext);

    /**
     * Returns the number of queries in this generator.
     * @param inputPage The input page containing values
     */
    int getPositionCount(Page inputPage);

}
