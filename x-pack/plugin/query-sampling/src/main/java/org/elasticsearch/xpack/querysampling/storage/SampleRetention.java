/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * Deletes sampled queries older than the retention, counted from when they were picked. {@link MasterTasks} runs it,
 * on the elected master only, as the other nodes would just repeat the same deletes.
 */
public final class SampleRetention {

    private static final Logger logger = LogManager.getLogger(SampleRetention.class);

    private static final TimeValue MAX_INTERVAL = TimeValue.timeValueHours(1);

    private final BiConsumer<DeleteByQueryRequest, ActionListener<BulkByPaginatedSearchResponse>> deleteByQuery;
    private final LongSupplier clock;
    private final TimeValue retention;

    private final LongAdder deleted = new LongAdder();

    public SampleRetention(
        BiConsumer<DeleteByQueryRequest, ActionListener<BulkByPaginatedSearchResponse>> deleteByQuery,
        LongSupplier clock,
        TimeValue retention
    ) {
        this.deleteByQuery = deleteByQuery;
        this.clock = clock;
        this.retention = retention;
    }

    /** A quarter of the retention, capped at an hour. */
    public static TimeValue interval(TimeValue retention) {
        TimeValue quarter = TimeValue.timeValueMillis(Math.max(1, retention.millis() / 4));
        return quarter.compareTo(MAX_INTERVAL) < 0 ? quarter : MAX_INTERVAL;
    }

    public void run() {
        DeleteByQueryRequest request = new DeleteByQueryRequest(QuerySamplingIndex.NAME).setQuery(
            QueryBuilders.rangeQuery("picked_at").lt(clock.getAsLong() - retention.millis())
        );
        // documents are updated all the time, one that changed under the delete is deleted in the next round
        request.setAbortOnVersionConflict(false);
        request.setRefresh(true);
        try {
            deleteByQuery.accept(request, ActionListener.wrap(response -> {
                deleted.add(response.getDeleted());
                if (response.getBulkFailures().isEmpty() == false || response.getSearchFailures().isEmpty() == false) {
                    logger.warn("some expired sampled queries could not be deleted");
                }
            }, e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException == false) { // nothing was ever sampled
                    logger.warn("failed to delete the expired sampled queries", e);
                }
            }));
        } catch (Exception e) {
            logger.warn("failed to delete the expired sampled queries", e);
        }
    }

    public long deleted() {
        return deleted.sum();
    }
}
