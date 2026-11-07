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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.querysampling.GroundTruthRunner;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * Computes ground truth of sampled queries unprompted, within the {@link CostBudget}: first the queries this node picked,
 * then ones nobody has touched for a while (likely from a node that went away; running nodes refresh theirs regularly).
 * <p>
 * Searches run as the plugin, since nobody is asking, so document level security is not applied; that is why the worker
 * is only scheduled while {@code sampling_cost_ratio} is above zero. At most one batch runs at a time.
 */
public final class GroundTruthWorker {

    private static final Logger logger = LogManager.getLogger(GroundTruthWorker.class);

    static final int MAX_BATCH = 10;
    static final TimeValue INTERVAL = TimeValue.timeValueSeconds(5);
    static final TimeValue ABANDONED_AFTER = TimeValue.timeValueMinutes(10);
    private static final double INITIAL_COST_MILLIS = 10;
    private static final double COST_SMOOTHING = 0.2;

    private final StoredGroundTruth storedGroundTruth;
    private final CostBudget budget;
    private final String samplerId;
    private final LongSupplier clock;

    private Scheduler.Cancellable scheduled; // guarded by this
    private boolean inFlight; // guarded by this
    private double costMillis = INITIAL_COST_MILLIS; // guarded by this

    private volatile String lastFailure;

    private final LongAdder computed = new LongAdder();
    private final LongAdder failed = new LongAdder();

    public GroundTruthWorker(
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> sampleSearch,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> sampleBulk,
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> exactSearch,
        NamedXContentRegistry registry,
        CostBudget budget,
        String samplerId,
        LongSupplier clock,
        Executor executor
    ) {
        this.budget = budget;
        this.samplerId = samplerId;
        this.clock = clock;
        this.storedGroundTruth = new StoredGroundTruth(
            sampleSearch,
            sampleBulk,
            (request, listener) -> exactSearch.accept(request, ActionListener.wrap(response -> {
                charge(response.getTook().millis(), DataState.isProbe(request));
                listener.onResponse(response);
            }, listener::onFailure)),
            registry,
            clock,
            executor
        );
    }

    /** Runs every few seconds, but only while {@code sampling_cost_ratio} is above 0, as without a budget there is nothing to do. */
    public void start(ThreadPool threadPool, Executor executor, ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.SAMPLING_COST_RATIO, ratio -> schedule(ratio > 0, threadPool, executor));
    }

    private synchronized void schedule(boolean wanted, ThreadPool threadPool, Executor executor) {
        if (wanted && scheduled == null) {
            scheduled = threadPool.scheduleWithFixedDelay(this::run, INTERVAL, executor);
        } else if (wanted == false && scheduled != null) {
            scheduled.cancel();
            scheduled = null;
        }
    }

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
        lastFailure = null;
        computed.add(result.computed());
        failed.add(result.failed());
        batchDone();
    }

    private void failed(Exception e) {
        // this repeats every few seconds while the cause stays, so only a new failure is logged at warn
        String failure = e.toString();
        if (failure.equals(lastFailure)) {
            logger.debug("failed again to compute the ground truth of stored queries", e);
        } else {
            lastFailure = failure;
            logger.warn("failed to compute the ground truth of stored queries (the same failure again is logged at debug)", e);
        }
        batchDone();
    }

    private synchronized void batchDone() {
        inFlight = false;
    }

    // only exact searches update the cost estimate, the state probes are too small to say what the next batch costs
    private synchronized void charge(long millis, boolean readsState) {
        double cost = Math.max(1, millis);
        budget.spend(cost);
        if (readsState == false) {
            costMillis = COST_SMOOTHING * cost + (1 - COST_SMOOTHING) * costMillis;
        }
    }

    public long computed() {
        return computed.sum();
    }

    public long failed() {
        return failed.sum();
    }
}
