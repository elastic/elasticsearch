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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.querysampling.sampling.SampleListener;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * Writes the queries picked for the sample to {@link QuerySamplingIndex}, so that nothing that matters is
 * only kept in the memory of a node that can go away.
 * <p>
 * The pipeline thread only queues the query. A batch is written when it is full or, if it is not, once the
 * flush interval has passed, so a quiet node does not sit on queries. One bulk request is in flight at a time,
 * and at most {@code maxPending} queries wait behind it: a node that cannot keep up drops queries, which is
 * harmless for the sample as which ones are dropped does not depend on the query.
 */
public final class SampleWriter implements SampleListener {

    private static final Logger logger = LogManager.getLogger(SampleWriter.class);

    private final String samplerId;
    private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulk;
    private final ThreadPool threadPool;
    private final Executor executor;
    private final LongSupplier clock;
    private final int maxBatch;
    private final int maxPending;
    private final TimeValue flushInterval;

    private final Queue<SampledQuery> pending = new ArrayDeque<>();
    private boolean timerScheduled;
    private boolean inFlight;

    private final LongAdder written = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final LongAdder dropped = new LongAdder();

    /**
     * @param samplerId     identifies this run of the sampler in the documents it writes
     * @param bulk          how bulk requests are sent, which decides who they are sent as
     * @param executor      where batches are built and sent, so that the pipeline thread is not used for it
     * @param clock         milliseconds since the epoch
     */
    public SampleWriter(
        String samplerId,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulk,
        ThreadPool threadPool,
        Executor executor,
        LongSupplier clock,
        int maxBatch,
        int maxPending,
        TimeValue flushInterval
    ) {
        this.samplerId = samplerId;
        this.bulk = bulk;
        this.threadPool = threadPool;
        this.executor = executor;
        this.clock = clock;
        this.maxBatch = maxBatch;
        this.maxPending = maxPending;
        this.flushInterval = flushInterval;
    }

    @Override
    public void onSampled(SampledQuery query) {
        synchronized (this) {
            if (pending.size() >= maxPending) {
                dropped.increment();
                return;
            }
            pending.add(query);
        }
        requestFlush();
    }

    /**
     * Makes sure the pending queries will be written: right away if there is a full batch, otherwise once the
     * flush interval has passed. Nothing is done while a bulk request is in flight, its completion asks again.
     */
    private void requestFlush() {
        boolean now;
        synchronized (this) {
            if (pending.isEmpty() || inFlight) {
                return;
            }
            if (pending.size() >= maxBatch) {
                now = true;
            } else if (timerScheduled == false) {
                timerScheduled = true;
                now = false;
            } else {
                return;
            }
        }
        if (now) {
            executor.execute(this::flush);
        } else {
            threadPool.schedule(this::flush, flushInterval, executor);
        }
    }

    private void flush() {
        List<SampledQuery> batch = new ArrayList<>();
        synchronized (this) {
            timerScheduled = false;
            if (inFlight || pending.isEmpty()) {
                return;
            }
            while (batch.size() < maxBatch && pending.isEmpty() == false) {
                batch.add(pending.poll());
            }
            inFlight = true;
        }
        send(batch);
    }

    private void send(List<SampledQuery> batch) {
        BulkRequest request = new BulkRequest();
        long now = clock.getAsLong();
        for (SampledQuery query : batch) {
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                request.add(
                    new IndexRequest(QuerySamplingIndex.NAME).id(SampleRecord.documentId(samplerId, query.fingerprint()))
                        .source(SampleRecord.document(builder, samplerId, query, now))
                );
            } catch (Exception e) {
                failed.increment();
                logger.debug("failed to build the document of a sampled query", e);
            }
        }
        if (request.numberOfActions() == 0) {
            completed();
            return;
        }
        ActionListener<BulkResponse> listener = ActionListener.runAfter(ActionListener.wrap(response -> {
            long failures = 0;
            for (BulkItemResponse item : response.getItems()) {
                failures += item.isFailed() ? 1 : 0;
            }
            failed.add(failures);
            written.add(request.numberOfActions() - failures);
            if (failures > 0) {
                logger.debug("[{}] sampled queries could not be written: {}", failures, response.buildFailureMessage());
            }
        }, e -> {
            failed.add(request.numberOfActions());
            logger.debug("failed to write sampled queries", e);
        }), this::completed);
        try {
            bulk.accept(request, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void completed() {
        synchronized (this) {
            inFlight = false;
        }
        requestFlush();
    }

    /**
     * Queries that were written to the index.
     */
    public long written() {
        return written.sum();
    }

    /**
     * Queries that could not be written, because the request failed or a document was rejected.
     */
    public long failed() {
        return failed.sum();
    }

    /**
     * Queries that were turned away because too many were waiting to be written.
     */
    public long dropped() {
        return dropped.sum();
    }
}
