/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.EscfBulkService;
import org.elasticsearch.action.bulk.EscfBulkWriter;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Streams an {@code INTO} query's pages through one {@link EscfBulkWriter}. Each driver has
 * its own producer. The writer bounds the bulks in flight and charges the query breaker and coordinating indexing pressure.
 */
public final class IntoPageWriter {
    static final EscfBulkWriter.Policy POLICY = new EscfBulkWriter.Policy(3, Operator.TARGET_PAGE_SIZE);

    /** Holds the query-scoped configuration, counters, and failure state across drivers for one {@code INTO} run. */
    public static final class Factory {
        public enum Indexing {
            /** Sends incoming pages to the destination as ESCF batches. */
            INDEX,
            /** Releases incoming pages without writing anything. */
            DISCARD
        }

        private final String destination;
        private final List<String> columns;
        private final List<DataType> types;
        @Nullable
        private final EscfBulkWriter writer;
        private final AtomicLong total = new AtomicLong();
        private final AtomicLong created = new AtomicLong();
        private final AtomicReference<Exception> failure = new AtomicReference<>();
        private final AtomicInteger openProducers = new AtomicInteger();
        private final List<IntoPageWriter> drivers = new CopyOnWriteArrayList<>();
        private volatile boolean settled;

        public static Factory createFactory(
            EscfBulkService bulks,
            Client client,
            CircuitBreaker breaker,
            String destination,
            List<Attribute> output,
            Indexing indexing
        ) {
            return new Factory(
                bulks,
                client,
                breaker,
                destination,
                Expressions.names(output),
                output.stream().map(Attribute::dataType).toList(),
                indexing
            );
        }

        private Factory(
            EscfBulkService bulks,
            Client client,
            CircuitBreaker breaker,
            String destination,
            List<String> columns,
            List<DataType> types,
            Indexing indexing
        ) {
            this.destination = destination;
            this.columns = columns;
            this.types = types;
            this.writer = indexing == Indexing.INDEX
                ? bulks.newWriter(client, breaker, destination, POLICY, this::onResponse, this::onCapacity)
                : null;
        }

        String destination() {
            return destination;
        }

        public IntoPageWriter create(DriverContext driverContext) {
            openProducers.incrementAndGet();
            IntoPageWriter pageWriter = new IntoPageWriter(
                this,
                writer == null ? null : writer.newProducer(),
                Objects.requireNonNull(driverContext),
                drivers.isEmpty()
            );
            drivers.add(pageWriter);
            return pageWriter;
        }

        private Exception onResponse(BulkResponse response) {
            for (BulkItemResponse item : response) {
                if (item.isFailed()) {
                    Exception cause = item.getFailure().getCause();
                    ElasticsearchStatusException failed = new ElasticsearchStatusException(
                        "INTO [{}] failed: {}",
                        item.getFailure().getStatus(),
                        cause,
                        destination,
                        cause.getMessage()
                    );
                    failure.compareAndSet(null, failed);
                    return failed;
                }
            }
            long rows = response.getItems().length;
            total.addAndGet(rows);
            created.addAndGet(rows);
            return null;
        }

        private Exception recordWriterFailure(Exception failed) {
            // A missing destination is wrapped so DataNodeRequestSender does not take it for a missing source shard and retry that
            boolean destinationUnavailable = TransportActions.isShardNotAvailableException(failed);
            failure.compareAndSet(
                null,
                destinationUnavailable
                    ? new ElasticsearchStatusException(
                        "INTO [{}] failed: {}",
                        ExceptionsHelper.status(failed),
                        failed,
                        destination,
                        failed.getMessage()
                    )
                    : failed
            );
            return failure.get();
        }

        private void onCapacity() {
            for (IntoPageWriter pageWriter : drivers) {
                pageWriter.wake();
            }
        }

        private void producerClosed() {
            if (openProducers.decrementAndGet() == 0) {
                writer.finish(ActionListener.wrap(ignored -> settled(null), this::settled));
            }
        }

        private void settled(Exception failed) {
            if (failed != null) {
                recordWriterFailure(failed);
            }
            settled = true;
            for (IntoPageWriter pageWriter : drivers) {
                pageWriter.settled();
            }
        }

        IntoStats stats() {
            return new IntoStats(total.get(), created.get());
        }

        /** Returns the first bulk or item failure recorded by this run, or {@code null} if every write succeeded. */
        @Nullable
        public Exception failure() {
            return failure.get();
        }

        /**
         * Fails the query when {@code target} cannot be written as a pre-built ESCF batch. Checked on the coordinator before the
         * query is dispatched, and again where a node plans the write, since the destination can change in between.
         */
        public static void requireBatchDestination(boolean batchIndexingSupported, ProjectMetadata project, String target) {
            // TODO: authorize write access to the destination before reporting destination-specific errors below, which would
            // otherwise reveal an inaccessible index's pipeline or routing. Tracked with the destination authorization in EsqlSession.
            if (batchIndexingSupported == false) {
                throw rejection("INTO [%s] requires batch indexing", target);
            }
            if (hasPipeline(project, target)) {
                throw rejection("INTO [%s] does not support ingest pipelines", target);
            }
            if (routesFromSource(project, target)) {
                throw rejection("INTO [%s] does not support indices that route from _source", target);
            }
        }

        private static boolean hasPipeline(ProjectMetadata project, String target) {
            IndexRequest probe = new IndexRequest(target);
            IngestService.setPipelineOnRequest(probe, IngestService.resolvePipelines(probe, probe, project, System.currentTimeMillis()));
            return IngestService.hasPipeline(probe);
        }

        private static ElasticsearchStatusException rejection(String format, Object... args) {
            return new ElasticsearchStatusException(Strings.format(format, args), RestStatus.BAD_REQUEST);
        }

        private static boolean routesFromSource(ProjectMetadata project, String target) {
            IndexAbstraction abstraction = project.getIndicesLookup().get(target);
            if (abstraction == null) {
                // Any matching template can create the index with source routing. Applying the template
                // is what resolves that routing, so a match is rejected.
                return MetadataIndexTemplateService.findV2Template(project, target, false) != null
                    || MetadataIndexTemplateService.findV1Templates(project, target, Boolean.FALSE).isEmpty() == false;
            }
            Index writeIndex = abstraction.getWriteIndex();
            return writeIndex != null
                && IndexRouting.fromIndexMetadata(project.getIndexSafe(writeIndex)) instanceof IndexRouting.ExtractFromSource;
        }
    }

    private final Factory sink;
    @Nullable
    private final EscfBulkWriter.Producer producer;
    private final DriverContext driverContext;
    // The factory's counters are query-wide, so only one driver reports them; the merge step sums every driver's row.
    private final boolean reportsStats;

    private boolean finished;
    private boolean producerClosed;
    private final AtomicBoolean asyncAction = new AtomicBoolean();
    // null whenever no driver is blocked on this writer; allocated in isBlocked() and cleared in wake().
    @Nullable
    private SubscribableListener<Void> waiting;

    private IntoPageWriter(Factory sink, @Nullable EscfBulkWriter.Producer producer, DriverContext driverContext, boolean reportsStats) {
        this.sink = sink;
        this.producer = producer;
        this.driverContext = driverContext;
        this.reportsStats = reportsStats;
        if (producer == null) {
            // Nothing is written, so there is nothing to settle.
            producerClosed = true;
        }
    }

    IntoStats stats() {
        return reportsStats ? sink.stats() : new IntoStats(0, 0);
    }

    /** Returns the first bulk or item failure recorded by this run, or {@code null} if every write succeeded. */
    @Nullable
    Exception failure() {
        return sink.failure();
    }

    public void consume(Page page) {
        try {
            if (producer != null && page.getPositionCount() > 0 && sink.failure.get() == null) {
                if (asyncAction.compareAndSet(false, true)) {
                    driverContext.addAsyncAction();
                }
                producer.add(new PageRows(page, sink.columns, sink.types, sink.destination), false);
                Exception failed = sink.writer.failure();
                if (failed != null) {
                    throw ExceptionsHelper.convertToRuntime(sink.recordWriterFailure(failed));
                }
            }
        } finally {
            page.releaseBlocks();
        }
    }

    public void finish() {
        closeProducer();
        wake(() -> finished = true);
    }

    public void close() {
        // finish() keeps the async action until the bulks settle. A failure never calls finish().
        if (producerClosed == false) {
            // Closing without finish() means the driver failed or was cancelled, so drop the rows not sent yet instead of
            // letting the producer hand its open batch to the writer to be flushed.
            sink.writer.close();
            closeProducer();
            releaseAsyncAction();
        }
    }

    private void closeProducer() {
        synchronized (this) {
            if (producerClosed) {
                return;
            }
            producerClosed = true;
        }
        producer.close();
        sink.producerClosed();
    }

    private void settled() {
        releaseAsyncAction();
        wake(() -> {});
    }

    private void releaseAsyncAction() {
        if (asyncAction.compareAndSet(true, false)) {
            driverContext.removeAsyncAction();
        }
    }

    private void wake() {
        wake(() -> {});
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

    public synchronized IsBlockedResult isBlocked() {
        if (mayContinue()) {
            return Operator.NOT_BLOCKED;
        }
        if (waiting == null) {
            waiting = new SubscribableListener<>();
        }
        return new IsBlockedResult(waiting, "INTO " + sink.destination);
    }

    private boolean mayContinue() {
        if (Thread.holdsLock(this) == false) {
            throw new IllegalStateException("mayContinue() must be called while holding this monitor");
        }
        if (producer == null) {
            return true;
        }
        return finished ? sink.settled : producer.hasCapacity();
    }

    public synchronized boolean isFinished() {
        return finished && (producer == null || sink.settled);
    }

    private record PageRows(Page page, List<String> columns, List<DataType> types, String destination) implements EscfBulkWriter.Rows {
        @Override
        public int count() {
            return page.getPositionCount();
        }

        @Override
        public void append(EscfBatchBuilder batch) {
            PageToEscfBatch.append(batch, page, columns, types);
        }

        @Override
        public IndexRequest request(int row) {
            return new IndexRequest(destination).opType(DocWriteRequest.OpType.CREATE);
        }
    }
}
