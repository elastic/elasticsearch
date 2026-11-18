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
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.EscfBulkService;
import org.elasticsearch.action.bulk.EscfBulkWriter;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.SubscribableListener;
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
import org.elasticsearch.compute.operator.PageConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.Index;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Streams an {@code INTO} query's pages through one {@link org.elasticsearch.action.bulk.EscfBulkWriter}. Each driver has
 * its own producer. The writer bounds the bulks in flight and charges the query breaker and coordinating indexing pressure.
 */
public final class IntoPageWriter implements PageConsumer {
    private static final EscfBulkWriter.Policy POLICY = new EscfBulkWriter.Policy(3, Operator.TARGET_PAGE_SIZE);

    /** Holds the query-scoped configuration, counters, and failure state across drivers for one {@code INTO} run. */
    public static final class Factory implements PageConsumer.Factory {
        private final String destination;
        private final List<String> columns;
        private final List<DataType> types;
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
            List<Attribute> output
        ) {
            return new Factory(
                bulks,
                client,
                breaker,
                destination,
                Expressions.names(output),
                output.stream().map(Attribute::dataType).toList()
            );
        }

        private Factory(
            EscfBulkService bulks,
            Client client,
            CircuitBreaker breaker,
            String destination,
            List<String> columns,
            List<DataType> types
        ) {
            this.destination = destination;
            this.columns = columns;
            this.types = types;
            this.writer = bulks.newWriter(client, breaker, destination, POLICY, this::onResponse, this::onCapacity);
        }

        @Override
        public IntoPageWriter create(DriverContext driverContext) {
            openProducers.incrementAndGet();
            IntoPageWriter pageWriter = new IntoPageWriter(this, writer.newProducer(), Objects.requireNonNull(driverContext));
            drivers.add(pageWriter);
            return pageWriter;
        }

        private Exception onResponse(BulkResponse response) {
            int createdRows = 0;
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
                createdRows++;
            }
            total.addAndGet(response.getItems().length);
            created.addAndGet(createdRows);
            return null;
        }

        private void onCapacity() {
            for (IntoPageWriter pageWriter : drivers) {
                pageWriter.wake();
            }
        }

        private void producerClosed() {
            if (openProducers.decrementAndGet() == 0) {
                writer.finish(ActionListener.wrap(ignored -> settled(), e -> settled()));
            }
        }

        private void settled() {
            Exception failed = writer.failure();
            if (failed != null) {
                failure.compareAndSet(null, failed);
            }
            settled = true;
            for (IntoPageWriter pageWriter : drivers) {
                pageWriter.settled();
            }
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
         * Fails the query when {@code target} cannot be written as a pre-built ESCF batch.
         * Checked once on the coordinator.
         */
        public static void requireBatchDestination(boolean batchIndexingSupported, ProjectMetadata project, String target) {
            if (batchIndexingSupported == false) {
                throw rejection(Strings.format("INTO [%s] requires batch indexing", target));
            }
            IndexRequest probe = new IndexRequest(target);
            IngestService.setPipelineOnRequest(probe, IngestService.resolvePipelines(probe, probe, project, System.currentTimeMillis()));
            if (IngestService.hasPipeline(probe)) {
                throw rejection(Strings.format("INTO [%s] does not support ingest pipelines", target));
            }
            if (routesFromSource(project, target)) {
                throw rejection(Strings.format("INTO [%s] does not support indices that route from _source", target));
            }
        }

        private static ElasticsearchStatusException rejection(String message) {
            return new ElasticsearchStatusException(message, RestStatus.BAD_REQUEST);
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
    private final EscfBulkWriter.Producer producer;
    private final DriverContext driverContext;

    private boolean finished;
    private boolean asyncAction;
    // null whenever no driver is blocked on this writer; allocated in isBlocked() and cleared in wake().
    @Nullable
    private SubscribableListener<Void> waiting;

    private IntoPageWriter(Factory sink, EscfBulkWriter.Producer producer, DriverContext driverContext) {
        this.sink = sink;
        this.producer = producer;
        this.driverContext = driverContext;
    }

    @Override
    public void consume(Page page) {
        try {
            if (page.getPositionCount() > 0 && sink.failure.get() == null) {
                if (asyncAction == false) {
                    driverContext.addAsyncAction();
                    asyncAction = true;
                }
                producer.add(new PageRows(page, sink.columns, sink.types, sink.destination), false);
                Exception failed = sink.writer.failure();
                if (failed != null) {
                    sink.failure.compareAndSet(null, failed);
                    if (failed instanceof RuntimeException runtime) {
                        throw runtime;
                    }
                    throw new IllegalStateException(failed);
                }
            }
        } finally {
            page.releaseBlocks();
        }
    }

    @Override
    public void finish() {
        producer.close();
        sink.producerClosed();
        wake(() -> finished = true);
    }

    private void settled() {
        releaseAsyncAction();
        wake(() -> {});
    }

    private void releaseAsyncAction() {
        if (asyncAction) {
            asyncAction = false;
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

    private boolean mayContinue() {
        if (Thread.holdsLock(this) == false) {
            throw new IllegalStateException("mayContinue() must be called while holding this monitor");
        }
        return finished ? sink.settled : producer.hasCapacity();
    }

    @Override
    public synchronized boolean isFinished() {
        return finished && sink.settled;
    }

    private record PageRows(Page page, List<String> columns, List<DataType> types, String destination) implements EscfBulkWriter.Rows {
        @Override
        public int count() {
            return page.getPositionCount();
        }

        @Override
        public void append(org.elasticsearch.escf.EscfBatchBuilder batch) {
            PageToEscfBatch.append(batch, page, columns, types);
        }

        @Override
        public IndexRequest request(int row) {
            return new IndexRequest(destination).opType(DocWriteRequest.OpType.CREATE);
        }
    }
}
