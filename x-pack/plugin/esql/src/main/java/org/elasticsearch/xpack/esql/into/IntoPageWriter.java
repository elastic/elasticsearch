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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.xcontent.XContentType;
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
 * Streams an {@code INTO} query's pages to its destination as bulk {@code create} requests. At most
 * {@link #MAX_IN_FLIGHT_BYTES} of source bytes wait for an acknowledgement across all drivers sharing the
 * same {@link Factory}, and each row's source is reserved on the request circuit breaker until that bulk
 * settles. The caller's {@link ThreadContext} is restored around each request.
 */
public final class IntoPageWriter implements PageConsumer {
    // visible for testing
    static final long MAX_IN_FLIGHT_BYTES = 3L * Operator.TARGET_PAGE_SIZE;

    private static final String BREAKER_LABEL = "into";

    /** Holds the query-scoped configuration, counters, and failure state across drivers for one {@code INTO} run. */
    public static final class Factory implements PageConsumer.Factory {
        private final Client client;
        private final Supplier<ThreadContext.StoredContext> caller;
        private final String destination;
        private final List<String> columns;
        private final List<DataType> types;
        private final boolean usePreBuiltBatch;
        private final AtomicLong total = new AtomicLong();
        private final AtomicLong created = new AtomicLong();
        private final AtomicReference<Exception> failure = new AtomicReference<>();
        // Tracks bytes in flight across all drivers sharing this factory.
        final AtomicLong factoryInFlightBytes = new AtomicLong();

        public static Factory createFactory(Client client, String destination, List<Attribute> output) {
            return createFactory(client, destination, output, false);
        }

        public static Factory createFactory(Client client, String destination, List<Attribute> output, boolean usePreBuiltBatch) {
            return new Factory(
                client,
                client.threadPool().getThreadContext().newRestorableContext(false),
                destination,
                Expressions.names(output),
                output.stream().map(Attribute::dataType).toList(),
                usePreBuiltBatch
            );
        }

        // visible for testing
        Factory(
            Client client,
            Supplier<ThreadContext.StoredContext> caller,
            String destination,
            List<String> columns,
            List<DataType> types,
            boolean usePreBuiltBatch
        ) {
            this.client = client;
            this.caller = caller;
            this.destination = destination;
            this.columns = columns;
            this.types = types;
            this.usePreBuiltBatch = usePreBuiltBatch;
        }

        @Override
        public IntoPageWriter create(DriverContext driverContext) {
            return new IntoPageWriter(this, Objects.requireNonNull(driverContext), LocalCircuitBreaker.forAsyncIo(driverContext.breaker()));
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
    private final CircuitBreaker breaker;

    private long driverInFlightBytes;
    private boolean finished;
    // null whenever no driver is blocked on this writer; allocated in isBlocked() and cleared in wake().
    @Nullable
    private SubscribableListener<Void> waiting;

    private IntoPageWriter(Factory sink, DriverContext driverContext, CircuitBreaker breaker) {
        this.sink = sink;
        this.driverContext = driverContext;
        this.breaker = breaker;
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

    private ReservedBulk bulkFor(Page page) {
        if (sink.usePreBuiltBatch) {
            return bulkForEscf(page);
        } else {
            return bulkForJson(page);
        }
    }

    private ReservedBulk bulkForEscf(Page page) {
        EscfBatch batch = PageToEscfBatch.convert(page, sink.columns, sink.types);
        long reserved = 0;
        try {
            long batchBytes = batch.ramBytesUsed();
            breaker.addEstimateBytesAndMaybeBreak(batchBytes, BREAKER_LABEL);
            reserved = batchBytes;

            BulkRequest bulk = new BulkRequest();
            int docCount = page.getPositionCount();
            for (int row = 0; row < docCount; row++) {
                IndexRequest request = new IndexRequest(sink.destination).opType(DocWriteRequest.OpType.CREATE);
                request.indexSource().setSourceRow(batch, row, XContentType.JSON);
                bulk.add(request);
            }
            if (bulk.numberOfActions() > 0) {
                bulk.setPreBuiltBatches(Map.of(sink.destination, batch));
            } else {
                batch.close();
            }
            return new ReservedBulk(bulk, reserved);
        } catch (Exception e) {
            release(reserved);
            batch.close();
            throw e;
        }
    }

    private ReservedBulk bulkForJson(Page page) {
        BulkRequest bulk = new BulkRequest();
        long reserved = 0;
        try {
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
                IndexRequest request = new IndexRequest(sink.destination).opType(DocWriteRequest.OpType.CREATE).source(source);
                long bytes = request.indexSource().byteLength();
                breaker.addEstimateBytesAndMaybeBreak(bytes, BREAKER_LABEL);
                reserved += bytes;
                bulk.add(request);
            }
            return new ReservedBulk(bulk, reserved);
        } catch (Exception e) {
            release(reserved);
            throw e;
        }
    }

    private void send(ReservedBulk reservedBulk) {
        BulkRequest bulk = reservedBulk.bulk();
        long bytes = reservedBulk.bytes();
        int actions = bulk.numberOfActions();
        if (actions == 0) {
            release(bytes);
            bulk.releasePreBuiltBatches();
            return;
        }
        synchronized (this) {
            driverInFlightBytes += bytes;
        }
        sink.factoryInFlightBytes.addAndGet(bytes);
        sink.total.addAndGet(actions);
        driverContext.addAsyncAction();
        ActionListener<BulkResponse> listener = ActionListener.runAfter(
            ActionListener.wrap(this::onBulkResponse, this::recordFailure),
            () -> {
                bulk.releasePreBuiltBatches();
                settle(bytes);
            }
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
    private void settle(long bytes) {
        release(bytes);
        driverContext.removeAsyncAction();
        // Decrement the factory-level counter before the per-driver wake so mayContinue() sees the new value.
        sink.factoryInFlightBytes.addAndGet(-bytes);
        wake(() -> driverInFlightBytes -= bytes);
    }

    private void release(long bytes) {
        breaker.addWithoutBreaking(-bytes, BREAKER_LABEL);
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

    // Blocking until driverInFlightBytes == 0 once finished to prevent a parked driver from spinning while bulks settle.
    private boolean mayContinue() {
        if (Thread.holdsLock(this) == false) {
            throw new IllegalStateException("mayContinue() must be called while holding this monitor");
        }
        return finished ? driverInFlightBytes == 0 : sink.factoryInFlightBytes.get() < MAX_IN_FLIGHT_BYTES;
    }

    @Override
    public synchronized boolean isFinished() {
        return finished && driverInFlightBytes == 0;
    }

    private record ReservedBulk(BulkRequest bulk, long bytes) {}
}
