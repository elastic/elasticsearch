/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;

/**
 * Collects common utility methods needed by various {@link DataExtractor} implementations
 */
public final class ExtractorUtils {

    private static final String EPOCH_MILLIS = "epoch_millis";

    private ExtractorUtils() {}

    /**
     * Combines a user query with a time range query.
     */
    public static QueryBuilder wrapInTimeRangeQuery(QueryBuilder userQuery, String timeField, long start, long end) {
        QueryBuilder timeQuery = new RangeQueryBuilder(timeField).gte(start).lt(end).format(EPOCH_MILLIS);
        return new BoolQueryBuilder().filter(userQuery).filter(timeQuery);
    }
}
