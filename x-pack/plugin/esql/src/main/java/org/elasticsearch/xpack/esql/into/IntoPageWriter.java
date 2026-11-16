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
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Streams an {@code INTO} query's pages to its destination as bulk {@code create} requests. At most
 * {@link #MAX_IN_FLIGHT_BYTES} of bytes wait for an acknowledgement per driver, and each bulk's bytes are
 * reserved on the request circuit breaker until that bulk settles. The caller's {@link ThreadContext} is
 * restored around each request.
 */
public final class IntoPageWriter implements PageConsumer {
    // visible for testing
    // Coordinating indexing pressure does not see ESCF pre-built batches (BulkRequest#ramBytesUsed sums
    // only item requests), so this cap is the only bound on pre-built batch bulks in flight.
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

        private Factory(
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

        /**
         * Returns {@code true} when the pre-built ESCF batch path may be used for the given target:
         * the destination has no ingest pipeline and does not route by extracting fields from {@code _source}.
         */
        public static boolean canUsePreBuiltBatch(ProjectMetadata project, String target) {
            IndexRequest probe = new IndexRequest(target);
            IngestService.setPipelineOnRequest(probe, IngestService.resolvePipelines(probe, probe, project, System.currentTimeMillis()));
            return IngestService.hasPipeline(probe) == false && routesFromSource(project, target) == false;
        }

        private static boolean routesFromSource(ProjectMetadata project, String target) {
            IndexAbstraction abstraction = project.getIndicesLookup().get(target);
            if (abstraction == null) {
                return MetadataIndexTemplateService.findV2Template(project, target, false) != null;
            }
            Index writeIndex = abstraction.getWriteIndex();
            return writeIndex != null
                && IndexRouting.fromIndexMetadata(project.getIndexSafe(writeIndex)) instanceof IndexRouting.ExtractFromSource;
        }
    }

    private final Factory sink;
    private final DriverContext driverContext;
    private final CircuitBreaker breaker;
    private final Recycler<BytesRef> recycler;

    private long inFlightBytes;
    private boolean finished;
    // null whenever no driver is blocked on this writer; allocated in isBlocked() and cleared in wake().
    @Nullable
    private SubscribableListener<Void> waiting;

    private IntoPageWriter(Factory sink, DriverContext driverContext, CircuitBreaker breaker) {
        this.sink = sink;
        this.driverContext = driverContext;
        this.breaker = breaker;
        this.recycler = driverContext.bigArrays().bytesRefRecycler();
    }

    @Override
    public void consume(Page page) {
        try {
            if (page.getPositionCount() > 0 && sink.failure.get() == null) {
                send(bulkFor(page));
            }
        } finally {
            page.releaseBlocks();
        }
    }

    private ReservedBulk bulkFor(Page page) {
        BulkRequest bulk = new BulkRequest();
        // convert may throw; on the ESCF path it releases its own charges before throwing.
        PageToEscfBatch.ConvertResult result = PageToEscfBatch.convert(
            page,
            sink.columns,
            sink.types,
            recycler,
            sink.usePreBuiltBatch ? breaker : null
        );
        bulk.setPreBuiltBatches(Map.of(sink.destination, result.batch()));
        long reserved = result.charged();
        try {
            for (int row = 0; row < result.batch().docCount(); row++) {
                IndexRequest request = new IndexRequest(sink.destination).opType(DocWriteRequest.OpType.CREATE);
                request.indexSource().setSourceRow(result.batch(), row, XContentType.JSON);
                if (sink.usePreBuiltBatch == false) {
                    inline(request);
                    long rowBytes = request.indexSource().byteLength();
                    breaker.addEstimateBytesAndMaybeBreak(rowBytes, BREAKER_LABEL);
                    reserved += rowBytes;
                }
                bulk.add(request);
            }
            if (sink.usePreBuiltBatch == false) {
                bulk.releasePreBuiltBatches();
            }
            return new ReservedBulk(bulk, reserved);
        } catch (Exception e) {
            release(reserved);
            bulk.releasePreBuiltBatches();
            throw e;
        }
    }

    private static void inline(IndexRequest request) {
        try {
            request.indexSource().ensureInlineSource();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void send(ReservedBulk reservedBulk) {
        BulkRequest bulk = reservedBulk.bulk();
        long bytes = reservedBulk.bytes();
        synchronized (this) {
            inFlightBytes += bytes;
        }
        sink.total.addAndGet(bulk.numberOfActions());
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
        wake(() -> inFlightBytes -= bytes);
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

    // Blocking until inFlightBytes == 0 once finished to prevent a parked driver from spinning while bulks settle.
    private boolean mayContinue() {
        if (Thread.holdsLock(this) == false) {
            throw new IllegalStateException("mayContinue() must be called while holding this monitor");
        }
        return finished ? inFlightBytes == 0 : inFlightBytes < MAX_IN_FLIGHT_BYTES;
    }

    @Override
    public synchronized boolean isFinished() {
        return finished && inFlightBytes == 0;
    }

    private record ReservedBulk(BulkRequest bulk, long bytes) {}
}
