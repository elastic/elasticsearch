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

/**
 * An interface to generates queries for the lookup and enrich operators.
 * This interface is used to retrieve queries based on a position index.
 */
public interface LookupEnrichQueryGenerator {

    /**
     * Returns the query at the given position.
     */
    @Nullable
    Query getQuery(int position, Page inputPage);

    /**
     * Returns the number of queries in this generator.
     */
    int getPositionCount(Page inputPage);

}
