/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.LongSupplier;

/**
 * Keeps the weights of the documents written by {@link SampleWriter} up to date. A query keeps arriving after it
 * was picked, so the multiplicity and the probabilities written with it go stale; they are written again
 * from time to time for as long as they change. Once the tracker has forgotten the query its counters are final,
 * the last change is written and the query is no longer looked at.
 * <p>
 * Only what is needed to compare the weights is kept for a query, not the query itself, so what it takes is small
 * and bounded by how many queries the tracker holds.
 */
public final class WeightsRefresher {

    private static final Logger logger = LogManager.getLogger(WeightsRefresher.class);

    /**
     * Updates of one query that failed one after the other, without the whole request failing, after which it is given
     * up on. A transient problem is over by then, and a query that cannot be updated should not hold up the others.
     */
    private static final int MAX_FAILURES = 5;

    /**
     * @param failures how many updates in a row were rejected
     */
    private record Written(TrackedQuery tracked, TrackedQuery.Weights last, int failures) {
        Written(TrackedQuery tracked, TrackedQuery.Weights last) {
            this(tracked, last, 0);
        }
    }

    private final String samplerId;
    private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulk;
    private final ThreadPool threadPool;
    private final Executor executor;
    private final LongSupplier clock;
    private final BiPredicate<QueryFingerprint, TrackedQuery> tracking;
    private final int maxBatch;
    private final TimeValue interval;

    private final Map<QueryFingerprint, Written> written = new HashMap<>();
    private boolean timerScheduled;
    private boolean inFlight;

    private final LongAdder refreshed = new LongAdder();
    private final LongAdder failed = new LongAdder();

    /**
     * @param tracking tells whether the tracker still holds the query, that is whether its weights can still change
     * @param maxBatch the most documents updated by one request
     * @param interval how long to wait between two rounds
     */
    public WeightsRefresher(
        String samplerId,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulk,
        ThreadPool threadPool,
        Executor executor,
        LongSupplier clock,
        BiPredicate<QueryFingerprint, TrackedQuery> tracking,
        int maxBatch,
        TimeValue interval
    ) {
        this.samplerId = samplerId;
        this.bulk = bulk;
        this.threadPool = threadPool;
        this.executor = executor;
        this.clock = clock;
        this.tracking = tracking;
        this.maxBatch = maxBatch;
        this.interval = interval;
    }

    /**
     * A query was written with the given weights, from now on they are kept up to date.
     */
    public void written(QueryFingerprint fingerprint, TrackedQuery tracked, TrackedQuery.Weights weights) {
        synchronized (this) {
            written.put(fingerprint, new Written(tracked, weights));
            if (timerScheduled || inFlight) {
                return;
            }
            timerScheduled = true;
        }
        threadPool.schedule(this::refresh, interval, executor);
    }

    private void refresh() {
        List<QueryFingerprint> fingerprints = new ArrayList<>();
        List<TrackedQuery.Weights> weights = new ArrayList<>();
        synchronized (this) {
            timerScheduled = false;
            if (inFlight) {
                return;
            }
            Iterator<Map.Entry<QueryFingerprint, Written>> iterator = written.entrySet().iterator();
            while (iterator.hasNext() && fingerprints.size() < maxBatch) {
                Map.Entry<QueryFingerprint, Written> entry = iterator.next();
                TrackedQuery.Weights current = entry.getValue().tracked().weights();
                if (current.equals(entry.getValue().last())) {
                    if (tracking.test(entry.getKey(), entry.getValue().tracked()) == false) {
                        iterator.remove(); // final, and the last state was written
                    }
                } else {
                    fingerprints.add(entry.getKey());
                    weights.add(current);
                }
            }
            if (fingerprints.isEmpty()) {
                scheduleNextRound(false);
                return;
            }
            inFlight = true;
        }
        send(fingerprints, weights);
    }

    private void send(List<QueryFingerprint> fingerprints, List<TrackedQuery.Weights> weights) {
        BulkRequest request = new BulkRequest();
        long now = clock.getAsLong();
        for (int i = 0; i < fingerprints.size(); i++) {
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                request.add(
                    new UpdateRequest(QuerySamplingIndex.NAME, SampleRecord.documentId(samplerId, fingerprints.get(i))).doc(
                        SampleRecord.weightsUpdate(builder, weights.get(i), now)
                    )
                );
            } catch (Exception e) {
                completed(fingerprints, weights, null);
                logger.debug("failed to build the update of a sampled query", e);
                return;
            }
        }
        ActionListener<BulkResponse> listener = ActionListener.wrap(response -> {
            Outcome[] outcomes = new Outcome[fingerprints.size()];
            Arrays.fill(outcomes, Outcome.FAILED);
            BulkItemResponse[] items = response.getItems();
            for (int i = 0; i < items.length && i < outcomes.length; i++) {
                if (items[i].isFailed() == false) {
                    outcomes[i] = Outcome.UPDATED;
                } else if (items[i].getFailure().getStatus() == RestStatus.NOT_FOUND) {
                    outcomes[i] = Outcome.GONE;
                }
            }
            completed(fingerprints, weights, outcomes);
        }, e -> {
            logger.debug("failed to refresh the weights of sampled queries", e);
            completed(fingerprints, weights, null);
        });
        try {
            bulk.accept(request, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * What happened to one update.
     */
    private enum Outcome {
        UPDATED,
        /** The document does not exist any more, so there is nothing to keep up to date. */
        GONE,
        FAILED
    }

    /**
     * What was updated is remembered. What failed is tried again in the next round, unless its document is gone or it
     * keeps failing: such a query would be first in line every time and keep the ones behind it from being updated.
     *
     * @param outcomes what happened to each update, or {@code null} if the request as a whole failed, which says
     *                 nothing about the queries and is not held against them
     */
    private void completed(List<QueryFingerprint> fingerprints, List<TrackedQuery.Weights> weights, Outcome[] outcomes) {
        boolean allSucceeded = outcomes != null;
        synchronized (this) {
            inFlight = false;
            for (int i = 0; i < fingerprints.size(); i++) {
                QueryFingerprint fingerprint = fingerprints.get(i);
                Outcome outcome = outcomes == null ? Outcome.FAILED : outcomes[i];
                if (outcome == Outcome.UPDATED) {
                    refreshed.increment();
                    TrackedQuery.Weights sent = weights.get(i);
                    written.computeIfPresent(fingerprint, (key, value) -> new Written(value.tracked(), sent));
                    continue;
                }
                failed.increment();
                allSucceeded = false;
                if (outcome == Outcome.GONE) {
                    written.remove(fingerprint);
                } else if (outcomes != null) {
                    written.computeIfPresent(
                        fingerprint,
                        (key, value) -> value.failures() + 1 >= MAX_FAILURES
                            ? null
                            : new Written(value.tracked(), value.last(), value.failures() + 1)
                    );
                }
            }
            // a full request means that more may be waiting, which is not the case to wait for after a failure
            scheduleNextRound(allSucceeded && fingerprints.size() >= maxBatch);
        }
    }

    private void scheduleNextRound(boolean immediately) {
        assert Thread.holdsLock(this);
        if (written.isEmpty() || timerScheduled) {
            return;
        }
        timerScheduled = true;
        if (immediately) {
            executor.execute(this::refresh);
        } else {
            threadPool.schedule(this::refresh, interval, executor);
        }
    }

    /**
     * Documents whose weights were updated.
     */
    public long refreshed() {
        return refreshed.sum();
    }

    /**
     * Updates that did not go through, whether they were tried again or not.
     */
    public long failed() {
        return failed.sum();
    }

    /**
     * Queries whose weights are still being kept up to date.
     */
    public synchronized int tracked() {
        return written.size();
    }
}
