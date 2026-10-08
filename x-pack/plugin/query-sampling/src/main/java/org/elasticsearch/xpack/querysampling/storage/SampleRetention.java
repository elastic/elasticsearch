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
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * Keeps {@link QuerySamplingIndex} short-term: sampled queries are deleted once they are older than the retention,
 * counted from when they were picked. Without it the index would grow for as long as sampling is on.
 * <p>
 * Only the elected master does it. Every node has the plugin, and having each of them delete the same documents
 * would only waste work, while the elected master is always exactly one node.
 */
public final class SampleRetention {

    private static final Logger logger = LogManager.getLogger(SampleRetention.class);

    private static final TimeValue MAX_INTERVAL = TimeValue.timeValueHours(1);

    private final BiConsumer<DeleteByQueryRequest, ActionListener<BulkByPaginatedSearchResponse>> deleteByQuery;
    private final BooleanSupplier electedMaster;
    private final LongSupplier clock;
    private final TimeValue retention;

    private final LongAdder deleted = new LongAdder();
    private final LongAdder failed = new LongAdder();

    /**
     * @param deleteByQuery  how the documents are deleted, as the plugin
     * @param electedMaster  whether this node is the elected master
     * @param clock          milliseconds since the epoch
     */
    public SampleRetention(
        BiConsumer<DeleteByQueryRequest, ActionListener<BulkByPaginatedSearchResponse>> deleteByQuery,
        BooleanSupplier electedMaster,
        LongSupplier clock,
        TimeValue retention
    ) {
        this.deleteByQuery = deleteByQuery;
        this.electedMaster = electedMaster;
        this.clock = clock;
        this.retention = retention;
    }

    /**
     * How often to look for what has expired: often enough that documents do not outlive the retention by much,
     * and at most once an hour, as a retention of days does not call for more.
     */
    public static TimeValue interval(TimeValue retention) {
        TimeValue quarter = TimeValue.timeValueMillis(Math.max(1, retention.millis() / 4));
        return quarter.compareTo(MAX_INTERVAL) < 0 ? quarter : MAX_INTERVAL;
    }

    /**
     * Starts running once per {@link #interval}, until the thread pool shuts down.
     */
    public Scheduler.Cancellable start(ThreadPool threadPool, Executor executor) {
        return threadPool.scheduleWithFixedDelay(this::run, interval(retention), executor);
    }

    /**
     * One round: deletes what has expired, if this node is the one to do it.
     */
    public void run() {
        if (electedMaster.getAsBoolean() == false) {
            return;
        }
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
                    failed.increment();
                    logger.debug("some expired sampled queries could not be deleted");
                }
            }, e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException == false) { // nothing was ever sampled
                    failed.increment();
                    logger.debug("failed to delete the expired sampled queries", e);
                }
            }));
        } catch (Exception e) {
            failed.increment();
            logger.debug("failed to delete the expired sampled queries", e);
        }
    }

    /**
     * Documents deleted by this node because they expired.
     */
    public long deleted() {
        return deleted.sum();
    }

    /**
     * Rounds that did not go through.
     */
    public long failed() {
        return failed.sum();
    }
}
