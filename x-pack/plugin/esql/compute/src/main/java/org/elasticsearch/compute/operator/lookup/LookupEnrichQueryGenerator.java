/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.apache.lucene.search.Query;
import org.elasticsearch.core.Nullable;

import java.util.Set;

/**
 * An interface to generates queries for the lookup and enrich operators.
 * This interface is used to retrieve queries based on a position index.
 */
public interface LookupEnrichQueryGenerator {

    /**
     * Returns the query at the given position.
     */
    @Nullable
    Query getQuery(int position);

    /**
     * Returns the number of queries in this generator
     */
    int getPositionCount();

    /**
     * Returns the set of field names that appear multiple times in queries.
     * This is used to avoid caching TermsEnum for fields that are used in multiple query clauses,
     * which can cause data issues when the same TermsEnum is reused for different queries.
     * <p>
     * By default, returns an empty set since most query generators don't have repeating fields.
     * Only {@code ExpressionQueryList} can have repeating fields.
     */
    default Set<String> fieldsWithMultipleQueries() {
        return Set.of();
    }

}
