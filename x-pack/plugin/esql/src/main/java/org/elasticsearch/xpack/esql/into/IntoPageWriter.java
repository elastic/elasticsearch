/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.action.ResponseValueUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Streams an {@code INTO} query's pages to its destination as bulk {@code create} requests, with at most
 * {@link #MAX_CONCURRENT_BULKS} in flight per driver, restoring the caller's {@link ThreadContext} around each request.
 */
public final class IntoPageWriter implements PageConsumer {
    // visible for testing
    static final int MAX_CONCURRENT_BULKS = 3;

    /** Holds the query-scoped configuration, counters, and failure state across drivers for one {@code INTO} run. */
    public static final class Factory implements PageConsumer.Factory {
        private final Client client;
        private final Supplier<ThreadContext.StoredContext> caller;
        private final String destination;
        private final List<String> columns;
        private final List<DataType> types;
        private final AtomicLong total = new AtomicLong();
        private final AtomicLong created = new AtomicLong();
        private final AtomicReference<Exception> failure = new AtomicReference<>();

        public static Factory createFactory(Client client, String destination, List<Attribute> output) {
            return new Factory(
                client,
                client.threadPool().getThreadContext().newRestorableContext(false),
                destination,
                Expressions.names(output),
                output.stream().map(Attribute::dataType).toList()
            );
        }

        private Factory(
            Client client,
            Supplier<ThreadContext.StoredContext> caller,
            String destination,
            List<String> columns,
            List<DataType> types
        ) {
            this.client = client;
            this.caller = caller;
            this.destination = destination;
            this.columns = columns;
            this.types = types;
        }

        @Override
        public IntoPageWriter create(DriverContext driverContext) {
            return new IntoPageWriter(this, Objects.requireNonNull(driverContext));
        }

        public IntoStats stats() {
            return new IntoStats(total.get(), created.get());
        }

        /** Returns the first bulk or item failure recorded by this run, or {@code null} if every write succeeded. */
        @Nullable
        public Exception failure() {
            return failure.get();
        }
    }

    private final Factory sink;
    private final DriverContext driverContext;

    private int inFlight;
    private boolean finished;
    // null whenever no driver is blocked on this writer; allocated in isBlocked() and cleared in wake().
    @Nullable
    private SubscribableListener<Void> waiting;

    private IntoPageWriter(Factory sink, DriverContext driverContext) {
        this.sink = sink;
        this.driverContext = driverContext;
    }

    @Override
    public void consume(Page page) {
        try {
            if (sink.failure.get() == null) {
                send(bulkFor(page));
            }
        } finally {
            page.releaseBlocks();
        }
    }

    private BulkRequest bulkFor(Page page) {
        BulkRequest bulk = new BulkRequest();
        // Render dates in UTC regardless of the query's time zone so the destination parses them unambiguously.
        Iterator<Iterator<Object>> rows = ResponseValueUtils.pagesToValues(sink.types, List.of(page), ZoneOffset.UTC);
        while (rows.hasNext()) {
            Iterator<Object> values = rows.next();
            Map<String, Object> source = new LinkedHashMap<>();
            for (String column : sink.columns) {
                Object value = values.next();
                if (value != null) {
                    source.put(column, value);
                }
            }
            bulk.add(new IndexRequest(sink.destination).opType(DocWriteRequest.OpType.CREATE).source(source));
        }
        return bulk;
    }

    private void send(BulkRequest bulk) {
        int actions = bulk.numberOfActions();
        if (actions == 0) {
            return;
        }
        synchronized (this) {
            inFlight++;
        }
        sink.total.addAndGet(actions);
        driverContext.addAsyncAction();
        ActionListener<BulkResponse> listener = ActionListener.runAfter(
            ActionListener.wrap(this::onBulkResponse, this::recordFailure),
            this::settled
        );
        try (ThreadContext.StoredContext ignored = sink.caller.get()) {
            sink.client.execute(TransportBulkAction.TYPE, bulk, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void onBulkResponse(BulkResponse response) {
        for (BulkItemResponse item : response) {
            if (item.isFailed()) {
                Exception cause = item.getFailure().getCause();
                recordFailure(
                    new ElasticsearchStatusException(
                        "INTO [{}] failed: {}",
                        item.getFailure().getStatus(),
                        cause,
                        sink.destination,
                        cause.getMessage()
                    )
                );
                return;
            }
        }
        sink.created.addAndGet(response.getItems().length);
    }

    private void recordFailure(Exception e) {
        sink.failure.compareAndSet(null, e);
    }

    // Complete the async action before unblocking the driver so a finishing driver sees zero pending actions, and do
    // so outside synchronized (this) because removing the last action on a failed driver runs its completion listener
    // synchronously on this thread.
    private void settled() {
        driverContext.removeAsyncAction();
        wake(() -> inFlight--);
    }

    @Override
    public void finish() {
        wake(() -> finished = true);
    }

    private void wake(Runnable change) {
        SubscribableListener<Void> release = null;
        synchronized (this) {
            change.run();
            if (mayContinue()) {
                release = waiting;
                waiting = null;
            }
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
        return new IsBlockedResult(waiting, "INTO " + sink.destination);
    }

    // Blocking until inFlight == 0 once finished keeps a parked driver from spinning while bulks settle.
    private boolean mayContinue() {
        if (Thread.holdsLock(this) == false) {
            throw new IllegalStateException("mayContinue() must be called while holding this monitor");
        }
        return finished ? inFlight == 0 : inFlight < MAX_CONCURRENT_BULKS;
    }

    @Override
    public synchronized boolean isFinished() {
        return finished && inFlight == 0;
    }
}
