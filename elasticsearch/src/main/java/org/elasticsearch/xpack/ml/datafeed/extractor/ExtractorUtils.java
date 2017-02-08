/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Arrays;

/**
 * Collects common utility methods needed by various {@link DataExtractor} implementations
 */
public final class ExtractorUtils {

    private static final Logger LOGGER = Loggers.getLogger(ExtractorUtils.class);
    private static final String EPOCH_MILLIS = "epoch_millis";

    private ExtractorUtils() {}

    /**
     * Combines a user query with a time range query.
     */
    public static QueryBuilder wrapInTimeRangeQuery(QueryBuilder userQuery, String timeField, long start, long end) {
        QueryBuilder timeQuery = new RangeQueryBuilder(timeField).gte(start).lt(end).format(EPOCH_MILLIS);
        return new BoolQueryBuilder().filter(userQuery).filter(timeQuery);
    }

    /**
     * Checks that a {@link SearchResponse} has an OK status code and no shard failures
     */
    public static void checkSearchWasSuccessful(String jobId, SearchResponse searchResponse) throws IOException {
        if (searchResponse.status() != RestStatus.OK) {
            throw new IOException("[" + jobId + "] Search request returned status code: " + searchResponse.status()
                    + ". Response was:\n" + searchResponse.toString());
        }
        ShardSearchFailure[] shardFailures = searchResponse.getShardFailures();
        if (shardFailures != null && shardFailures.length > 0) {
            LOGGER.error("[{}] Search request returned shard failures: {}", jobId, Arrays.toString(shardFailures));
            throw new IOException("[" + jobId + "] Search request returned shard failures; see more info in the logs");
        }
        int unavailableShards = searchResponse.getTotalShards() - searchResponse.getSuccessfulShards();
        if (unavailableShards > 0) {
            throw new IOException("[" + jobId + "] Search request encountered [" + unavailableShards + "] unavailable shards");
        }
    }
}
