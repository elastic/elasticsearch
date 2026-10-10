/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.groundtruth;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.querysampling.GroundTruthRunner;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * Computes the ground truth of the sampled queries without being asked to, within the {@link CostBudget} of the node.
 * <p>
 * A node takes care of the queries it picked itself, and when none of those is left, of the ones that nobody has
 * touched for a long time, which are most likely those of a node that has gone away. The stored weights of the
 * queries of a node that is running are refreshed regularly, so they never look abandoned.
 * <p>
 * Every exact search scans the index, so it is done as the plugin and not as a user: nobody is asking, and nobody's
 * privileges are the right ones. That also means that document level security is not applied, which is why the
 * worker only runs when {@code sampling_cost_ratio} is set above zero.
 * <p>
 * At most one batch is being computed at any time. How many queries a batch holds is how many the budget can afford,
 * judging by what the latest exact searches cost.
 */
public final class GroundTruthWorker {

    private static final Logger logger = LogManager.getLogger(GroundTruthWorker.class);

    static final int MAX_BATCH = 10;
    static final TimeValue INTERVAL = TimeValue.timeValueSeconds(5);
    /**
     * A query whose document nobody has touched for this long belongs to nobody.
     */
    static final TimeValue ABANDONED_AFTER = TimeValue.timeValueMinutes(10);
    private static final double INITIAL_COST_MILLIS = 10;
    private static final double COST_SMOOTHING = 0.2;

    private final StoredGroundTruth storedGroundTruth;
    private final CostBudget budget;
    private final String samplerId;
    private final LongSupplier clock;

    private boolean inFlight; // guarded by this
    private double costMillis = INITIAL_COST_MILLIS; // guarded by this

    private final LongAdder computed = new LongAdder();
    private final LongAdder failed = new LongAdder();

    /**
     * @param sampleSearch searches the index of the sample, as the plugin
     * @param sampleBulk   updates the index of the sample, as the plugin
     * @param exactSearch  runs the exact searches, as the plugin
     * @param samplerId    the id of the sampler of this node, which its queries are stored with
     * @param clock        milliseconds since the epoch
     */
    public GroundTruthWorker(
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> sampleSearch,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> sampleBulk,
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> exactSearch,
        NamedXContentRegistry registry,
        CostBudget budget,
        String samplerId,
        LongSupplier clock
    ) {
        this.budget = budget;
        this.samplerId = samplerId;
        this.clock = clock;
        this.storedGroundTruth = new StoredGroundTruth(
            sampleSearch,
            sampleBulk,
            (request, listener) -> exactSearch.accept(request, ActionListener.wrap(response -> {
                charge(response.getTook().millis());
                listener.onResponse(response);
            }, listener::onFailure)),
            registry,
            clock
        );
    }

    /**
     * Starts to look for work every few seconds, until the thread pool shuts down.
     */
    public Scheduler.Cancellable start(ThreadPool threadPool, Executor executor) {
        return threadPool.scheduleWithFixedDelay(this::run, INTERVAL, executor);
    }

    /**
     * One round: computes what the budget affords, if there is something to compute and no batch is still running.
     */
    public void run() {
        int batch;
        synchronized (this) {
            if (inFlight || budget.ratio() <= 0) {
                return;
            }
            batch = budget.affordable(costMillis, MAX_BATCH);
            if (batch == 0) {
                return;
            }
            inFlight = true;
        }
        QueryBuilder own = QueryBuilders.termQuery("sampler_id", samplerId);
        storedGroundTruth.compute(batch, own, ActionListener.wrap(result -> {
            if (result.computed() + result.failed() == 0) {
                // all of its own are done, what is left is what nobody looks after
                QueryBuilder abandoned = QueryBuilders.rangeQuery("updated_at").lt(clock.getAsLong() - ABANDONED_AFTER.millis());
                storedGroundTruth.compute(batch, abandoned, ActionListener.wrap(this::finished, this::failed));
            } else {
                finished(result);
            }
        }, this::failed));
    }

    private void finished(GroundTruthRunner.Result result) {
        record(result);
        synchronized (this) {
            inFlight = false;
        }
    }

    private void failed(Exception e) {
        logger.debug("failed to compute the ground truth of stored queries", e);
        synchronized (this) {
            inFlight = false;
        }
    }

    private void record(GroundTruthRunner.Result result) {
        computed.add(result.computed());
        failed.add(result.failed());
    }

    /**
     * An exact search took {@code millis}: it is paid for, and tells what the next ones are likely to cost.
     */
    private synchronized void charge(long millis) {
        double cost = Math.max(1, millis);
        budget.spend(cost);
        costMillis = COST_SMOOTHING * cost + (1 - COST_SMOOTHING) * costMillis;
    }

    /**
     * Queries that got their ground truth from this worker.
     */
    public long computed() {
        return computed.sum();
    }

    /**
     * Queries whose ground truth could not be computed or stored.
     */
    public long failed() {
        return failed.sum();
    }
}
