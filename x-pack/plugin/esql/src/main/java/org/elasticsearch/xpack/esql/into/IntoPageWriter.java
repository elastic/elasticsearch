/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.action.ResponseValueUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Streams an {@code INTO} query's pages to its destination as bulk {@code create} requests, with at most
 * {@link #MAX_CONCURRENT_BULKS} in flight, restoring the caller's {@link ThreadContext} around each request.
 */
public final class IntoPageWriter implements PageConsumer {

    // visible for testing
    static final int MAX_CONCURRENT_BULKS = 3;

    private final Client client;
    private final Supplier<ThreadContext.StoredContext> caller;
    private final String destination;
    private final String[] columns;
    private final ResponseValueUtils.BlockValueExtractor[] extractors;

    private final IntoStats stats = new IntoStats();
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    @Nullable
    private DriverContext driverContext;
    private int inFlight;
    private boolean finished;
    private boolean done;
    @Nullable
    private SubscribableListener<Void> waiting;

    public IntoPageWriter(Client client, String destination, List<Attribute> output) {
        this.client = client;
        this.caller = client.threadPool().getThreadContext().newRestorableContext(false);
        this.destination = destination;
        this.columns = new String[output.size()];
        this.extractors = new ResponseValueUtils.BlockValueExtractor[output.size()];
        for (int i = 0; i < output.size(); i++) {
            Attribute attribute = output.get(i);
            columns[i] = attribute.name();
            // Render dates in UTC regardless of the query's time zone so the destination parses them unambiguously.
            extractors[i] = ResponseValueUtils.valueExtractorFor(attribute.dataType(), ZoneOffset.UTC);
        }
    }

    public IntoStats stats() {
        return stats;
    }

    @Nullable
    public Exception failure() {
        return failure.get();
    }

    @Override
    public synchronized void bind(DriverContext driverContext) {
        if (this.driverContext != null && this.driverContext != driverContext) {
            throw new IllegalStateException("INTO writes from a single driver");
        }
        this.driverContext = driverContext;
    }

    @Override
    public void accept(Page page) {
        try {
            if (failure.get() != null) {
                return;
            }
            BulkRequest bulk = bulkFor(page);
            if (bulk.numberOfActions() > 0) {
                send(bulk);
            }
        } finally {
            page.releaseBlocks();
        }
    }

    private BulkRequest bulkFor(Page page) {
        BulkRequest bulk = new BulkRequest();
        BytesRef scratch = new BytesRef();
        for (int position = 0; position < page.getPositionCount(); position++) {
            Map<String, Object> source = new HashMap<>();
            for (int column = 0; column < columns.length; column++) {
                Object value = valueAt(page.getBlock(column), extractors[column], position, scratch);
                if (value != null) {
                    source.put(columns[column], value);
                }
            }
            bulk.add(new IndexRequest(destination).opType(DocWriteRequest.OpType.CREATE).source(source));
        }
        synchronized (this) {
            stats.countRows(page.getPositionCount());
        }
        return bulk;
    }

    @Nullable
    private static Object valueAt(Block block, ResponseValueUtils.BlockValueExtractor extractor, int position, BytesRef scratch) {
        if (block.isNull(position)) {
            return null;
        }
        int count = block.getValueCount(position);
        int start = block.getFirstValueIndex(position);
        if (count == 1) {
            return extractor.extract(block, start, scratch);
        }
        List<Object> values = new ArrayList<>(count);
        for (int i = start; i < start + count; i++) {
            values.add(extractor.extract(block, i, scratch));
        }
        return values;
    }

    private void send(BulkRequest bulk) {
        synchronized (this) {
            inFlight++;
            if (driverContext != null) {
                driverContext.addAsyncAction();
            }
        }
        try (ThreadContext.StoredContext ignored = caller.get()) {
            client.execute(TransportBulkAction.TYPE, bulk, ActionListener.wrap(this::onBulkResponse, this::onBulkFailure));
        } catch (Exception e) {
            onBulkFailure(e);
        }
    }

    private void onBulkResponse(BulkResponse response) {
        synchronized (this) {
            for (BulkItemResponse item : response) {
                if (item.isFailed()) {
                    BulkItemResponse.Failure itemFailure = item.getFailure();
                    Throwable cause = itemFailure.getCause();
                    String reason = cause != null ? cause.getMessage() : itemFailure.getMessage();
                    recordFailure(
                        new ElasticsearchStatusException("INTO [{}] failed: {}", itemFailure.getStatus(), cause, destination, reason)
                    );
                } else if (item.getResponse().getResult() == DocWriteResponse.Result.CREATED) {
                    stats.countCreated();
                }
            }
        }
        settled();
    }

    private void onBulkFailure(Exception e) {
        recordFailure(e);
        settled();
    }

    private void recordFailure(Exception e) {
        failure.compareAndSet(null, e);
    }

    private void settled() {
        SubscribableListener<Void> release;
        synchronized (this) {
            inFlight--;
            if (driverContext != null) {
                driverContext.removeAsyncAction();
            }
            if (finished && inFlight == 0) {
                done = true;
            }
            release = mayContinue() ? takeWaiting() : null;
        }
        if (release != null) {
            release.onResponse(null);
        }
    }

    @Override
    public void finish() {
        SubscribableListener<Void> release;
        synchronized (this) {
            finished = true;
            if (inFlight == 0) {
                done = true;
            }
            release = mayContinue() ? takeWaiting() : null;
        }
        if (release != null) {
            release.onResponse(null);
        }
    }

    @Override
    public synchronized IsBlockedResult isBlocked() {
        if (mayContinue()) {
            return Operator.NOT_BLOCKED;
        }
        if (waiting == null) {
            waiting = new SubscribableListener<>();
        }
        return new IsBlockedResult(waiting, "INTO " + destination);
    }

    // Returning a listener whenever finished == true && done == false keeps a parked driver from spinning while bulks settle.
    private boolean mayContinue() {
        assert Thread.holdsLock(this);
        return finished ? done : inFlight < MAX_CONCURRENT_BULKS;
    }

    @Nullable
    private SubscribableListener<Void> takeWaiting() {
        assert Thread.holdsLock(this);
        SubscribableListener<Void> release = waiting;
        waiting = null;
        return release;
    }

    @Override
    public synchronized boolean isFinished() {
        return done;
    }

    @Override
    public void close() {}
}
