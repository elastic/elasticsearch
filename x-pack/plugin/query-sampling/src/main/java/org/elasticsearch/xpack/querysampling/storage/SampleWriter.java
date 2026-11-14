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
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * Writes picked queries to {@link QuerySamplingIndex} in batches, one bulk request in flight at a time. A node that
 * cannot keep up drops queries beyond {@code maxPending}, which is harmless as which ones are dropped does not
 * depend on the query.
 */
public final class SampleWriter {

    private static final Logger logger = LogManager.getLogger(SampleWriter.class);

    private final String samplerId;
    private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulk;
    private final ThreadPool threadPool;
    private final Executor executor;
    private final LongSupplier clock;
    private final WrittenListener onWritten;
    private final int maxBatch;
    private final int maxPending;
    private final TimeValue flushInterval;

    private final Queue<SampledQuery> pending = new ArrayDeque<>();
    private boolean timerScheduled;
    private boolean inFlight;

    private final LongAdder written = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final LongAdder dropped = new LongAdder();

    public SampleWriter(
        String samplerId,
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulk,
        ThreadPool threadPool,
        Executor executor,
        LongSupplier clock,
        WrittenListener onWritten,
        int maxBatch,
        int maxPending,
        TimeValue flushInterval
    ) {
        this.samplerId = samplerId;
        this.bulk = bulk;
        this.threadPool = threadPool;
        this.executor = executor;
        this.clock = clock;
        this.onWritten = onWritten;
        this.maxBatch = maxBatch;
        this.maxPending = maxPending;
        this.flushInterval = flushInterval;
    }

    @FunctionalInterface
    public interface WrittenListener {
        void written(QueryFingerprint fingerprint, TrackedQuery tracked, TrackedQuery.Weights weights);
    }

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

    // flushes right away on a full batch, else after the flush interval; while a bulk is in flight its completion asks again
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
        // the documents that made it into the request, in the order of the request
        List<SampledQuery> sent = new ArrayList<>();
        List<TrackedQuery.Weights> weights = new ArrayList<>();
        long now = clock.getAsLong();
        for (SampledQuery query : batch) {
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                // read before the document is built, so that if they change in between it shows as a change later
                TrackedQuery.Weights written = query.tracked().weights();
                String id = SampleRecord.documentId(samplerId, query.fingerprint().hex(), query.eventId());
                request.add(new IndexRequest(QuerySamplingIndex.NAME).id(id).source(SampleRecord.document(builder, samplerId, query, now)));
                sent.add(query);
                weights.add(written);
            } catch (Exception e) {
                failed.increment();
                logger.warn("failed to build the document of a sampled query", e);
            }
        }
        if (request.numberOfActions() == 0) {
            completed();
            return;
        }
        ActionListener<BulkResponse> listener = ActionListener.runAfter(ActionListener.wrap(response -> {
            long failures = 0;
            BulkItemResponse[] items = response.getItems();
            assert items.length == request.numberOfActions() : "a bulk response has an item for each action";
            for (int i = 0; i < items.length; i++) {
                if (items[i].isFailed()) {
                    failures++;
                } else if (sent.get(i).eventId() == null) { // an event is final, there is nothing to refresh
                    SampledQuery query = sent.get(i);
                    onWritten.written(query.fingerprint(), query.tracked(), weights.get(i));
                }
            }
            failed.add(failures);
            written.add(request.numberOfActions() - failures);
            if (failures > 0) {
                logger.warn("[{}] sampled queries could not be written: {}", failures, response.buildFailureMessage());
            }
        }, e -> {
            failed.add(request.numberOfActions());
            logger.warn("failed to write sampled queries", e);
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

    public long written() {
        return written.sum();
    }

    public long failed() {
        return failed.sum();
    }

    public long dropped() {
        return dropped.sum();
    }
}
